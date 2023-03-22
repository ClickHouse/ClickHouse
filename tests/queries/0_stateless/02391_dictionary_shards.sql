drop dictionary if exists dict;
drop dictionary if exists dict_10;
drop dictionary if exists dict_10_uint8;
drop dictionary if exists dict_10_string;
drop dictionary if exists dict_10_incremental;
drop dictionary if exists complex_dict_10;
drop table if exists data;
drop table if exists data_string;
drop table if exists complex_data;

create table data (key UInt64, value UInt16) engine=Memory() as select number, number from numbers(1e5);
create table data_string (key String, value UInt16) engine=Memory() as select 'foo' || number::String, number from numbers(1e5);
create table complex_data (k1 UInt64, k2 UInt64, value UInt16) engine=Memory() as select number, number, number from numbers(1e5);

-- { echoOn }
create dictionary dict (key UInt64, value UInt16) primary key key source(clickhouse(table data)) layout(sparse_hashed()) lifetime(0);
show create dict;
system reload dictionary dict;
select element_count from system.dictionaries where database = currentDatabase() and name = 'dict';
select count() from data where dictGetUInt16('dict', 'value', key) != value;

create dictionary dict_10 (key UInt64, value UInt16) primary key key source(clickhouse(table data)) layout(sparse_hashed(shards 10)) lifetime(0);
show create dict_10;
system reload dictionary dict_10;
select element_count from system.dictionaries where database = currentDatabase() and name = 'dict_10';
select count() from data where dictGetUInt16('dict_10', 'value', key) != value;

create dictionary dict_10_uint8 (key UInt8, value UInt16) primary key key source(clickhouse(table data)) layout(sparse_hashed(shards 10)) lifetime(0);
show create dict_10_uint8;
system reload dictionary dict_10_uint8;
select element_count from system.dictionaries where database = currentDatabase() and name = 'dict_10';
select count() from data where dictGetUInt16('dict_10_uint8', 'value', key) != value;

create dictionary dict_10_string (key String, value UInt16) primary key key source(clickhouse(table data_string)) layout(sparse_hashed(shards 10)) lifetime(0);
show create dict_10_string;
system reload dictionary dict_10_string; -- { serverError CANNOT_PARSE_TEXT }

create dictionary dict_10_incremental (key UInt64, value UInt16) primary key key source(clickhouse(table data_last_access update_field last_access)) layout(sparse_hashed(shards 10)) lifetime(0);
system reload dictionary dict_10_incremental; -- { serverError BAD_ARGUMENTS }

create dictionary complex_dict_10 (k1 UInt64, k2 UInt64, value UInt16) primary key k1, k2 source(clickhouse(table complex_data)) layout(complex_key_sparse_hashed(shards 10)) lifetime(0);
system reload dictionary complex_dict_10;
select element_count from system.dictionaries where database = currentDatabase() and name = 'complex_dict_10';
select count() from complex_data where dictGetUInt16('complex_dict_10', 'value', (k1, k2)) != value;
