DROP TABLE IF EXISTS realtimedrep;
DROP TABLE IF EXISTS realtimedistributed;
DROP TABLE IF EXISTS realtimebuff;

CREATE TABLE realtimedrep(amount Int64,transID String,userID String,appID String,appName String,transType String,orderSource String,nau String,fau String,transactionType String,supplier String,fMerchant String,bankConnCode String,reqDate DateTime) ENGINE = MergeTree PARTITION BY toDate(reqDate) ORDER BY transID SETTINGS index_granularity = 8192;
CREATE TABLE realtimedistributed(amount Int64,transID String,userID String,appID String,appName String,transType String,orderSource String,nau String,fau String,transactionType String,supplier String,fMerchant String,bankConnCode String,reqDate DateTime) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), realtimedrep, rand());
CREATE TABLE realtimebuff(amount Int64,transID String,userID String,appID String,appName String,transType String,orderSource String,nau String,fau String,transactionType String,supplier String,fMerchant String,bankConnCode String,reqDate DateTime) ENGINE = Buffer(currentDatabase(), 'realtimedistributed', 16, 3600, 36000, 10000, 1000000, 10000000, 100000000);

insert into realtimebuff (amount,transID,userID,appID,appName,transType,orderSource,nau,fau,transactionType,supplier,fMerchant,bankConnCode,reqDate) values (100, '200312000295032','200223000028708','14', 'Data','1', '20','1', '0','123','abc', '1234a','ZPVBIDV', 1598256583);

select sum(amount) = 100 from realtimebuff;

OPTIMIZE TABLE realtimebuff;
select sum(amount) IN (100, 200) from realtimebuff;

SYSTEM FLUSH DISTRIBUTED realtimedistributed;
select sum(amount) = 200 from realtimebuff;

DROP TABLE realtimedrep;
DROP TABLE realtimedistributed;
DROP TABLE realtimebuff;
