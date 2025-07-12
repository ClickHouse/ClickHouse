
SET distributed_ddl_output_mode='none';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/test/databases/{database}/test_02068/1', 's1', 'r1');
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('/test/databases/{database}/test_02068/2', 's1', 'r1');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.dest_table (x Int64) ENGINE=ReplicatedMergeTree ORDER BY x;

CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_2:Identifier}.rmv_to_re_dest REFRESH EVERY 1 SECOND TO {CLICKHOUSE_DATABASE_1:Identifier}.dest_table AS SELECT number*15 AS x FROM numbers(2); -- { serverError BAD_ARGUMENTS }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
