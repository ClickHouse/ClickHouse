
DROP TABLE IF EXISTS test.set_index_not;

CREATE TABLE test.set_index_not
(   name String, status Enum8('alive' = 0, 'rip' = 1),
    INDEX idx_status status TYPE set(2) GRANULARITY 1
)
ENGINE = MergeTree()  ORDER BY name  SETTINGS index_granularity = 8192;

insert into test.set_index_not values ('Jon','alive'),('Ramsey','rip');

select * from test.set_index_not where status!='rip';
select * from test.set_index_not where NOT (status ='rip');

DROP TABLE test.set_index_not;