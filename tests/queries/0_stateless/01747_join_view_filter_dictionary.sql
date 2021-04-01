drop table if exists summing_table01747;
drop view if exists rates01747;
drop view if exists agg_view01747;
drop table if exists dictst01747;
drop DICTIONARY if exists default.dict01747;

CREATE TABLE summing_table01747
 (
    some_name               String,
    user_id                 UInt64,
    amount                  Int64,
    currency                String
 )
ENGINE = SummingMergeTree()
ORDER BY (some_name);

CREATE VIEW rates01747 AS
   SELECT 'USD' as from_currency, 'EUR' as to_currency, 1.2 as rates01747;

insert into summing_table01747 values ('name', 2, 20, 'USD'),('name', 1, 10, 'USD');

create table dictst01747(some_name String, field1 String, field2 UInt8) Engine = Memory
as select 'name', 'test', 33;

CREATE DICTIONARY default.dict01747 (some_name String, field1 String, field2 UInt8)
PRIMARY KEY some_name SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 
TABLE dictst01747 DB currentDatabase() USER 'default')) 
LIFETIME(MIN 0 MAX 0) LAYOUT(COMPLEX_KEY_HASHED());


CREATE VIEW agg_view01747 AS
  SELECT
    summing_table01747.some_name as some_name,
    dictGet('default.dict01747', 'field1', tuple(some_name)) as field1,
    dictGet('default.dict01747', 'field2', tuple(some_name)) as field2,
    rates01747.rates01747 as rates01747
  FROM summing_table01747
  ANY LEFT JOIN rates01747
    ON rates01747.from_currency = summing_table01747.currency;

select * from agg_view01747;

SELECT field2 FROM agg_view01747 WHERE field1 = 'test';

drop table summing_table01747;
drop view rates01747;
drop view agg_view01747;
drop table dictst01747;
drop DICTIONARY default.dict01747;
