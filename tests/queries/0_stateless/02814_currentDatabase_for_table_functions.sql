-- Based on https://github.com/ClickHouse/ClickHouse/issues/52436
-- Test that inserts performed via Buffer table engine land into destination table.
-- { echoOn }

DROP TABLE IF EXISTS null_table;
DROP TABLE IF EXISTS null_table_buffer;
DROP TABLE IF EXISTS null_mv;
DROP VIEW IF EXISTS number_view;

CREATE TABLE null_table (number UInt64) ENGINE = Null;
CREATE VIEW number_view as SELECT * FROM numbers(10) as tb;
CREATE MATERIALIZED VIEW null_mv Engine = Log AS SELECT * FROM null_table LEFT JOIN number_view as tb USING number;

CREATE TABLE null_table_buffer (number UInt64) ENGINE = Buffer(currentDatabase(), null_table, 1, 1, 1, 100, 200, 10000, 20000);
INSERT INTO null_table_buffer VALUES (1);

-- OPTIMIZE query should flush Buffer table, but still it is not guaranteed
-- (see the comment StorageBuffer::optimize)
-- But the combination of OPTIMIZE + sleep + OPTIMIZE should be enough.
OPTIMIZE TABLE null_table_buffer;
SELECT sleep(1) FORMAT Null;
OPTIMIZE TABLE null_table_buffer;

-- Insert about should've landed into `null_mv`
SELECT count() FROM null_mv;
