-- {CLICKHOUSE_DATABASE} must not be re-engined: with --database it is shared by every test in
-- the client. {CLICKHOUSE_DATABASE_2} is shared too, so both DROPs are required.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('some/path/' || {CLICKHOUSE_DATABASE_2:String} || '/replicated_database_test', 'shard_1', 'replica_1') SETTINGS max_broken_tables_ratio=1;
SELECT engine_full FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_2:String};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
