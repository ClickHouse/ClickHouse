

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = PostgreSQL('google.com:5432', 'dummy', 'dummy', 'dummy');

-- Should quickly return result instead of wasting time in connection
SELECT DISTINCT(name) FROM system.tables WHERE engine!='PostgreSQL' AND name='COLUMNS';

-- Enigne of system.tables in fact means storage name, so View should not get filtered 
SELECT DISTINCT(name) FROM system.tables WHERE engine='View' and name='COLUMNS';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

-- Originally wanna test MySQL as well, but different from PostgreSQL, building the connection to a inaccessible 
-- MySQL database is not instant (sync mode). But filtering is still useful in a network partition.
-- CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = MySQL('google.com:3306', 'dummy', 'dummy', 'dummy');
-- SELECT DISTINCT(name) FROM system.tables WHERE engine!='MySQL' AND name='COLUMNS';
-- SELECT DISTINCT(name) FROM system.tables WHERE engine='View' and name='COLUMNS';
-- DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};