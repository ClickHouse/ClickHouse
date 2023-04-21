DROP TABLE IF EXISTS srv_account_parts;
DROP TABLE IF EXISTS etl_batch;

CREATE TABLE srv_account_parts(
    shard_num UInt16,
    account_ids Array(Int64)
)ENGINE = ReplacingMergeTree
ORDER BY shard_num
as select * from values ((0,[]),(1,[1,2,3]),(2,[1,2,3]),(3,[1]));

CREATE TABLE etl_batch(
    batch_id UInt64,
    batch_start DateTime,
    batch_start_day Date DEFAULT toDate(batch_start),
    batch_load DateTime,
    total_num_records UInt32,
    etl_server_id Int32,
    account_id UInt64,
    shard_num UInt16
)ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(batch_start_day)
ORDER BY (batch_id, etl_server_id, account_id);

insert into etl_batch(batch_id, batch_start, batch_load, total_num_records, etl_server_id, account_id, shard_num)
select number batch_id, 
       toDateTime('2022-01-01') + INTERVAL 23 HOUR batch_start,
       batch_start batch_load,
       333 total_num_records,
       1 etl_server_id,
       number%3+1 account_id,
       1 shard_num
from numbers(1000);

insert into etl_batch(batch_id, batch_start, batch_load, total_num_records, etl_server_id, account_id, shard_num)
select number+2000 batch_id, 
       toDateTime('2022-01-01') + INTERVAL 23 HOUR batch_start,
       batch_start batch_load,
       333 total_num_records,
       1 etl_server_id,
       number%3+1 account_id,
       2 shard_num
from numbers(1000);

insert into etl_batch(batch_id, batch_start, batch_load, total_num_records, etl_server_id, account_id, shard_num)
select number+4000 batch_id, 
       toDateTime('2022-01-01') + INTERVAL 3 HOUR batch_start,
       batch_start batch_load,
       3333 total_num_records,
       1 etl_server_id,
       2 account_id,
       2 shard_num
from numbers(1000);

insert into etl_batch(batch_id, batch_start, batch_load, total_num_records, etl_server_id, account_id, shard_num)
select number+6000 batch_id, 
       toDateTime('2022-01-01') + INTERVAL 23 HOUR  batch_start,
       batch_start batch_load,
       333 total_num_records,
       1 etl_server_id,
       1 account_id,
       2 shard_num
from numbers(1000);

insert into etl_batch(batch_id, batch_start, batch_load, total_num_records, etl_server_id, account_id, shard_num)
select number+8000 batch_id, 
       toDateTime('2022-01-01') + INTERVAL 23 HOUR batch_start,
       batch_start batch_load,
       1000 total_num_records,
       1 etl_server_id,
       3 account_id,
       3 shard_num
from numbers(1000);

CREATE OR REPLACE VIEW v_num_records_by_node_bias_acc as
SELECT shard_num,
       arrayJoin(account_ids) AS account_id,
       records_24h,
       records_12h,
       IF (b = '',-100,xbias) AS bias,
       IF (bias > 10,0,IF (bias > 0,1,IF (bias < -10,301,300))) AS sbias
FROM srv_account_parts
  LEFT JOIN (SELECT account_id,
                    shard_num,
                    records_24h,
                    records_12h,
                    xbias,
                    'b' AS b
             FROM (SELECT account_id,
                          groupArray((shard_num,records_24h,records_12h)) AS ga,
                          arraySum(ga.2) AS tot24,
                          arraySum(ga.3) AS tot12,
                          arrayMap(i ->(((((i.2)*LENGTH(ga))*100) / tot24) - 100),ga) AS bias24,
                          arrayMap(i ->(((((i.3)*LENGTH(ga))*100) / tot12) - 100),ga) AS bias12,
                          arrayMap((i,j,k) ->(i,IF (tot12 = 0,0,IF (ABS(j) > ABS(k),j,k))),ga,bias24,bias12) AS a_bias
                   FROM (SELECT shard_num,
                                toInt64(account_id) AS account_id,
                                SUM(total_num_records) AS records_24h,
                                sumIf(total_num_records,batch_load >(toDateTime('2022-01-02') -(3600*12))) AS records_12h
                         FROM etl_batch FINAL PREWHERE (batch_start_day >= (toDate('2022-01-02') - 2)) AND (batch_load > (toDateTime('2022-01-02') - (3600*24)))
                         where (shard_num, account_id) in (select shard_num, arrayJoin(account_ids) from srv_account_parts)
                         GROUP BY shard_num,account_id)
                   GROUP BY account_id) 
                   ARRAY JOIN (a_bias.1).1 AS shard_num,a_bias.2 AS xbias, (a_bias.1).2 AS records_24h, (a_bias.1).3 AS records_12h
            ) s USING (shard_num,account_id);

select account_id, shard_num, round(bias,4) 
from v_num_records_by_node_bias_acc
order by  account_id, shard_num, bias;

select '---------';

SELECT a AS b FROM (SELECT 0 a) s LEFT JOIN (SELECT 0 b) t USING (b);

SELECT arrayJoin(a) AS b FROM (SELECT [0] a) s LEFT JOIN (SELECT 0 b) t USING (b);

DROP TABLE srv_account_parts;
DROP TABLE etl_batch;
