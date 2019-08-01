SET allow_experimental_data_skipping_indices = 1;

CREATE TABLE a
(   name String, status Enum8('alive' = 0, 'rip' = 1),
    INDEX idx_status status TYPE set(2) GRANULARITY 1
)
ENGINE = MergeTree()  ORDER BY name  SETTINGS index_granularity = 8192;

insert into a values ('Jon','alive'),('Ramsey','rip');

select * from a where status!='rip';
select * from a where NOT (status ='rip');
