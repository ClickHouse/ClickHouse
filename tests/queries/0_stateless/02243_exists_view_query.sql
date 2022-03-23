DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (x UInt8) ENGINE = TinyLog;

DROP VIEW IF EXISTS materialized_view;
CREATE MATERIALIZED VIEW materialized_view ENGINE = TinyLog AS SELECT * FROM test_table;

EXISTS VIEW materialized_view;
EXISTS MATERIALIZED VIEW materialized_view;
EXISTS LIVE VIEW materialized_view;

DROP VIEW materialized_view;

EXISTS VIEW materialized_view;
EXISTS MATERIALIZED VIEW materialized_view;
EXISTS LIVE VIEW materialized_view;

DROP TABLE test_table;
