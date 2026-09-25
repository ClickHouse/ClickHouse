-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_nullable_key_final;

-- A Row key with a Nullable element must be treated as unsafe by the parts
-- splitter of FINAL reads, exactly like a Tuple key with a Nullable element.
CREATE TABLE row_nullable_key_final (r Row(x Nullable(UInt64), y UInt64))
ENGINE = ReplacingMergeTree ORDER BY r SETTINGS allow_nullable_key = 1;

INSERT INTO row_nullable_key_final VALUES ((1, 2)), ((NULL, 3));
INSERT INTO row_nullable_key_final VALUES ((NULL, 3)), ((2, 1));
INSERT INTO row_nullable_key_final VALUES ((NULL, 4)), ((1, 2));

SELECT r FROM row_nullable_key_final FINAL ORDER BY r SETTINGS max_threads = 4, max_final_threads = 4;
SELECT r FROM merge(currentDatabase(), '^row_nullable_key_final$') FINAL GROUP BY ALL ORDER BY ALL;

DROP TABLE row_nullable_key_final;
