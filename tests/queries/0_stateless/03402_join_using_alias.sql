set enable_analyzer=1;

CREATE OR REPLACE TABLE t0 (c0 Int, c1 Int ALIAS 1) ENGINE = Memory;
SELECT c0 FROM remote('localhost', currentDatabase(), 't0') tx JOIN t0 USING (c1); -- { serverError BAD_ARGUMENTS }
