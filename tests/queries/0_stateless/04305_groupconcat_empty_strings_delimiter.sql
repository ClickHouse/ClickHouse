SELECT groupConcat(',')(arrayJoin(['', '', 'a']));
SELECT groupConcat(',')(arrayJoin(['', '', '']));
SELECT groupConcat(',')(arrayJoin(['', 'a', '']));

SELECT groupConcat(',', 3)(arrayJoin(['', '', 'a']));

DROP TABLE IF EXISTS t_groupconcat_empty;
CREATE TABLE t_groupconcat_empty (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_groupconcat_empty SELECT '' FROM numbers(1000);

SELECT length(groupConcat(',')(s)) FROM t_groupconcat_empty SETTINGS max_threads = 8, max_block_size = 100;
SELECT length(groupConcat(',', 1000)(s)) FROM t_groupconcat_empty SETTINGS max_threads = 8, max_block_size = 100;

DROP TABLE t_groupconcat_empty;
