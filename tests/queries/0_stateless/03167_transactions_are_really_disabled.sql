-- Tags: no-encrypted-storage

DROP TABLE IF EXISTS mv_table;
DROP TABLE IF EXISTS null_table;

CREATE TABLE null_table (str String) ENGINE = Null;
CREATE MATERIALIZED VIEW mv_table (str String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/transactions_disabled_rmt', '{replica}') ORDER BY str AS SELECT str AS str FROM null_table;

SET implicit_transaction=1;
set throw_on_unsupported_query_inside_transaction=0;

INSERT INTO null_table VALUES ('test'); --{serverError NOT_IMPLEMENTED}

DROP TABLE IF EXISTS mv_table;
DROP TABLE IF EXISTS null_table;
