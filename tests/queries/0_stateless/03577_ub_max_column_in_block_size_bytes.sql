CREATE TABLE t0 (c0 Bool) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO TABLE t0 (c0) VALUES (TRUE);

SELECT c0 FROM t0 SETTINGS preferred_max_column_in_block_size_bytes = 18446744073709551615;
