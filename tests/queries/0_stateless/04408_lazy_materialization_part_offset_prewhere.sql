DROP ROW POLICY IF EXISTS repro_pol ON repro;
DROP TABLE IF EXISTS repro;

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;

CREATE TABLE repro (Id UInt64, EventId UInt64, flag UInt8) ENGINE = MergeTree ORDER BY Id;
INSERT INTO repro SELECT number, number, number % 2 FROM numbers(1000);

SELECT EventId FROM repro WHERE _part_starting_offset + _part_offset < 100 ORDER BY Id DESC LIMIT 10;
SELECT EventId FROM repro PREWHERE _part_starting_offset + _part_offset < 100 ORDER BY Id DESC LIMIT 10;

CREATE ROW POLICY repro_pol ON repro USING _part_offset < 100 TO ALL;

-- The `_part_offset` is consumed by the row-level filter, no PREWHERE.
SELECT EventId FROM repro ORDER BY Id DESC LIMIT 10;
-- Both filters consume the offset columns.
SELECT EventId FROM repro PREWHERE _part_starting_offset + _part_offset < 100 ORDER BY Id DESC LIMIT 10;

DROP ROW POLICY repro_pol ON repro;

CREATE ROW POLICY repro_pol ON repro USING _part_starting_offset + _part_offset < 100 TO ALL;

-- Both offset virtual columns are consumed by the row-level filter, no PREWHERE.
SELECT EventId FROM repro ORDER BY Id DESC LIMIT 10;

-- The offset filter additionally appears in PREWHERE: both filters consume the offset columns.
SELECT EventId FROM repro WHERE _part_starting_offset + _part_offset < 50 ORDER BY Id DESC LIMIT 10;

-- Virtual columns are consumed by the row-level filter, PREWHERE refers unrelated columns.
SELECT EventId FROM repro WHERE flag = 0 ORDER BY Id DESC LIMIT 10;
SELECT EventId FROM repro PREWHERE flag = 0 ORDER BY Id DESC LIMIT 10;

DROP ROW POLICY repro_pol ON repro;
CREATE ROW POLICY repro_pol ON repro USING flag = 0 TO ALL;

-- PREWHERE consumes the virtual columns, but the row-level refers unrelated columns.
SELECT EventId FROM repro WHERE _part_starting_offset + _part_offset < 50 ORDER BY Id DESC LIMIT 10;
SELECT EventId FROM repro PREWHERE _part_starting_offset + _part_offset < 50 ORDER BY Id DESC LIMIT 10;

DROP ROW POLICY repro_pol ON repro;

-- The bare virtual column is the PREWHERE filter column itself.
SELECT EventId FROM repro PREWHERE _part_offset ORDER BY Id DESC LIMIT 10;
-- The bare virtual column is both the PREWHERE filter column and a projected column.
SELECT EventId, _part_offset FROM repro PREWHERE _part_offset ORDER BY Id LIMIT 5;

CREATE ROW POLICY repro_pol ON repro USING _part_offset TO ALL;

-- The bare virtual column is the row-level filter column itself.
SELECT EventId FROM repro ORDER BY Id DESC LIMIT 10;
-- The bare virtual column is both the row-level and the PREWHERE filter column.
SELECT EventId FROM repro PREWHERE _part_offset ORDER BY Id DESC LIMIT 10;
-- The bare virtual column is the row-level filter column and a PREWHERE expression input.
SELECT EventId FROM repro PREWHERE _part_offset < 100 ORDER BY Id DESC LIMIT 10;

DROP ROW POLICY repro_pol ON repro;
DROP TABLE repro;
