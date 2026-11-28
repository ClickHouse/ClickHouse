-- The scan-time string filter (`apply_string_filters_during_scan`) replaces non-matching values with
-- empty strings, and the row is rejected only after PREWHERE has been evaluated. On-fly mutations
-- (`apply_mutations_on_fly`) are executed as steps ahead of PREWHERE, so their expressions would observe
-- the substituted empty strings of every row that the substring condition later rejects. Therefore the
-- optimization must be disabled while such mutations are applied. Here `throwIf` would fire for every
-- non-matching row if the value had been replaced.

DROP TABLE IF EXISTS t_string_filter_mutation;

CREATE TABLE t_string_filter_mutation (id UInt32, s String, y UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;

INSERT INTO t_string_filter_mutation
SELECT number, if(number % 3 = 0, 'lorem needle ipsum ' || toString(number), 'nothing interesting ' || toString(number)), number
FROM numbers(10000);

SYSTEM STOP MERGES t_string_filter_mutation;

SET mutations_sync = 0, apply_mutations_on_fly = 1, apply_string_filters_during_scan = 1, optimize_move_to_prewhere = 0;

-- A pending `DELETE` whose condition reads the filtered column (it deletes nothing).
ALTER TABLE t_string_filter_mutation DELETE WHERE throwIf(empty(s), 'the value was replaced') = 1;

SELECT count(), sum(id) FROM t_string_filter_mutation PREWHERE s LIKE '%needle%';
SELECT count(), sum(id) FROM t_string_filter_mutation PREWHERE s LIKE '%needle%' WHERE y >= 0;

-- A pending `UPDATE` of another column whose expression reads the filtered column.
ALTER TABLE t_string_filter_mutation UPDATE y = throwIf(empty(s), 'the value was replaced') WHERE 1;

SELECT count(), sum(id), sum(y) FROM t_string_filter_mutation PREWHERE s LIKE '%needle%';

DROP TABLE t_string_filter_mutation;
