DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SETTINGS distributed_ddl_task_timeout=42;
SYSTEM FLUSH LOGS query_log;
SELECT Settings['distributed_ddl_task_timeout'] FROM system.query_log where event_date >= yesterday() AND event_time >= now() - 600 AND
    current_database = currentDatabase() and
    type != 'QueryStart' and
    query like 'CREATE DATABASE % SETTINGS %';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};