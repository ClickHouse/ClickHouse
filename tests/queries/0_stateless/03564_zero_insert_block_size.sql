-- Test for issue #83620

CREATE TABLE t0 (c0 Int) ENGINE = Memory;
INSERT INTO TABLE t0 (c0) SETTINGS max_insert_block_size = 0 VALUES (1); -- { clientError BAD_ARGUMENTS }
DROP TABLE t0;
