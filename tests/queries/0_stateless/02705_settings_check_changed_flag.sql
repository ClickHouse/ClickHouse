---SettingFieldNumber
SELECT changed from system.settings where name = 'mysql_max_rows_to_insert';
SET mysql_max_rows_to_insert = 123123;

select changed from system.settings where name = 'mysql_max_rows_to_insert';
set mysql_max_rows_to_insert = 123123;
select changed from system.settings where name = 'mysql_max_rows_to_insert';
set mysql_max_rows_to_insert = 65536;
select changed from system.settings where name = 'mysql_max_rows_to_insert';

---SettingAutoWrapper 

select changed from system.settings where name = 'insert_quorum';
set insert_quorum = 123123;
select changed from system.settings where name = 'insert_quorum';
set insert_quorum = 123123;
select changed from system.settings where name = 'insert_quorum';
set insert_quorum = 0;
select changed from system.settings where name = 'insert_quorum';

---SettingFieldMaxThreads 

select changed from system.settings where name = 'max_alter_threads';
set max_alter_threads = 123123;
select changed from system.settings where name = 'max_alter_threads';
set max_alter_threads = 123123;
select changed from system.settings where name = 'max_alter_threads';
set max_alter_threads = 0;
select changed from system.settings where name = 'max_alter_threads';

---SettingFieldTimespanUnit

select changed from system.settings where name = 'drain_timeout';
set drain_timeout = 123123;
select changed from system.settings where name = 'drain_timeout';
set drain_timeout = 123123;
select changed from system.settings where name = 'drain_timeout';
set drain_timeout = 3;
select changed from system.settings where name = 'drain_timeout';


---SettingFieldChar

select changed from system.settings where name = 'format_csv_delimiter';
set format_csv_delimiter = ',';
select changed from system.settings where name = 'format_csv_delimiter';
set format_csv_delimiter = ',';
select changed from system.settings where name = 'format_csv_delimiter';
set format_csv_delimiter = ',';
select changed from system.settings where name = 'format_csv_delimiter';


---SettingFieldURI

select changed from system.settings where name = 'format_avro_schema_registry_url';
set format_avro_schema_registry_url = 'https://github.com/ClickHouse/ClickHouse/tree/master/src/Core';
select changed from system.settings where name = 'format_avro_schema_registry_url';
set format_avro_schema_registry_url = 'https://github.com/ClickHouse/ClickHouse/tree/master/src/Core';
select changed from system.settings where name = 'format_avro_schema_registry_url';
set format_avro_schema_registry_url = '';
select changed from system.settings where name = 'format_avro_schema_registry_url';


--- SettingFieldEnum

select changed from system.settings where name = 'output_format_orc_compression_method';
set output_format_orc_compression_method = 'none';
select changed from system.settings where name = 'output_format_orc_compression_method';
set output_format_orc_compression_method = 'none';
select changed from system.settings where name = 'output_format_orc_compression_method';
set output_format_orc_compression_method = 'lz4';
select changed from system.settings where name = 'output_format_orc_compression_method';

--- SettingFieldMultiEnum

select changed from system.settings where name = 'join_algorithm';
set join_algorithm = 'auto,direct';
select changed from system.settings where name = 'join_algorithm';
set join_algorithm = 'auto,direct';
select changed from system.settings where name = 'join_algorithm';
set join_algorithm = 'default';
select changed from system.settings where name = 'join_algorithm';
