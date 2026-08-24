SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE=Ordinary; -- Different inner table name with Atomic

set allow_deprecated_syntax_for_merge_tree=1;
create table {CLICKHOUSE_DATABASE:Identifier}.my_table ENGINE = MergeTree(day, (day), 8192) as select today() as day, 'mystring' as str;
show tables from {CLICKHOUSE_DATABASE:Identifier};
create materialized view {CLICKHOUSE_DATABASE:Identifier}.my_materialized_view ENGINE = MergeTree(day, (day), 8192) as select * from {CLICKHOUSE_DATABASE:Identifier}.my_table;
show tables from {CLICKHOUSE_DATABASE:Identifier};
select * from {CLICKHOUSE_DATABASE:Identifier}.my_materialized_view;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
