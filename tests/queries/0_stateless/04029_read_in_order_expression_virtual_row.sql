DROP TABLE IF EXISTS t_vrow_reverse;

CREATE TABLE t_vrow_reverse
(
    ts DateTime,
    user_id String,
    score_a UInt64,
    score_b UInt64,
    value UInt64
)
ENGINE = MergeTree
ORDER BY (toUnixTimestamp(ts), sipHash64(user_id), if(user_id != '', score_a + score_b, 0));

-- Insert into separate parts so read-in-order with virtual rows kicks in
INSERT INTO t_vrow_reverse VALUES ('2024-01-01 00:00:00', 'alice', 1, 2, 3);
INSERT INTO t_vrow_reverse VALUES ('2024-01-02 00:00:00', 'bob', 2, 3, 4);
INSERT INTO t_vrow_reverse VALUES ('2024-01-03 00:00:00', 'charlie', 3, 4, 5);

SET read_in_order_two_level_merge_threshold = DEFAULT, optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, read_in_order_use_virtual_row_per_block = 1;

SELECT value FROM t_vrow_reverse ORDER BY toUnixTimestamp(ts) ASC LIMIT 10;
SELECT value FROM t_vrow_reverse ORDER BY toUnixTimestamp(ts) DESC LIMIT 10;
SELECT ignore(value) FROM t_vrow_reverse ORDER BY sign(toUnixTimestamp(ts)) ASC LIMIT 10;
SELECT ignore(value) FROM t_vrow_reverse ORDER BY sign(toUnixTimestamp(ts)) DESC LIMIT 10;

DROP TABLE t_vrow_reverse;
