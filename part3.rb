require 'rubygems'
require 'sinatra'
require 'mongo'
require 'mongo_mapper'

MongoMapper.connection = Mongo::Connection.new('localhost', 27017)
MongoMapper.database = 'ipDB'
datab = Mongo::Connection.new.db('ipDB')
datac = datab['ip_info']

class IPInfo
  include MongoMapper::Document
  key :ipAddress, String
  timestamps!
end

class IPMapReduce
  include MongoMapper::Document

  key :thisYear, Float
  key :thisMonth, Float
  key :thisDay, Float
  key :counter, Float

  def self.map
    <<-MAP
      function() { emit( { ipAddress: this.ipAddress, thisYear: this.created_at.getFullYear(), thisMonth: this.created_at.getMonth(), thisDay: this.created_at.getDate() }, { counter: 1 } ); }
    MAP
  end

  def self.reduce
    <<-REDUCE
function(key, values) { var counter = 0; values.forEach(function(val) { counter += val['counter']; }); return {counter: counter}; }
    REDUCE
  end

  def self.build
    IPInfo.collection.map_reduce(map, reduce, { out: "ip_mapreduce" })
  end
end

get '/' do

  thisRequest = IPInfo.new(:ipAddress => request.ip)
  thisRequest.save

  IPMapReduce.build

  b = "Hello World" + "\n"
  datab["ip_mapreduce"].find.each { |item1|
    item = item1['_id']
    item2 = item1['value']
    b = b + (item['thisMonth']+1).to_int.to_s + "-" + item['thisDay'].to_int.to_s + "-" + item['thisYear'].to_int.to_s + " = " + item2['counter'].to_int.to_s + "\n" 
  }

  b

end
