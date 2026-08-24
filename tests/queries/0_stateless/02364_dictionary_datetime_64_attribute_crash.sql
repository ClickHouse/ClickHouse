create table dat (blockNum Decimal(10,0), eventTimestamp DateTime64(9)) Engine=MergeTree() primary key eventTimestamp;
insert into dat values (1, '2022-01-24 02:30:00.008122000');

CREATE DICTIONARY datDictionary
(
    `blockNum` Decimal(10, 0),
    `eventTimestamp` DateTime64(9)
)
PRIMARY KEY blockNum
SOURCE(CLICKHOUSE(TABLE 'dat'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT());

select (select eventTimestamp from datDictionary);
select count(*) from dat where eventTimestamp >= (select eventTimestamp from datDictionary);
