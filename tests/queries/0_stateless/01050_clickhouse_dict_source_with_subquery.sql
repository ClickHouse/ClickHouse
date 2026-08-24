
drop dictionary if exists {CLICKHOUSE_DATABASE:Identifier}.test_dict_01051_d;
drop table if exists {CLICKHOUSE_DATABASE:Identifier}.test_01051_d;
drop table if exists {CLICKHOUSE_DATABASE:Identifier}.test_view_01051_d;

create table {CLICKHOUSE_DATABASE:Identifier}.test_01051_d (key UInt64, value String) engine = MergeTree order by key;
create view {CLICKHOUSE_DATABASE:Identifier}.test_view_01051_d (key UInt64, value String) as select k2 + 1 as key, v2 || '_x' as value from (select key + 2 as k2, value || '_y' as v2 from test_01051_d);

insert into {CLICKHOUSE_DATABASE:Identifier}.test_01051_d values (1, 'a');

create dictionary {CLICKHOUSE_DATABASE:Identifier}.test_dict_01051_d (key UInt64, value String) primary key key source(clickhouse(host 'localhost' port '9000' user 'default' password '' db currentDatabase() table 'test_view_01051_d')) layout(flat()) lifetime(100500);

select dictGet({CLICKHOUSE_DATABASE:String} || '.test_dict_01051_d', 'value', toUInt64(4));

drop dictionary if exists {CLICKHOUSE_DATABASE:Identifier}.test_dict_01051_d;
drop table if exists {CLICKHOUSE_DATABASE:Identifier}.test_01051_d;
drop table if exists {CLICKHOUSE_DATABASE:Identifier}.test_view_01051_d;
