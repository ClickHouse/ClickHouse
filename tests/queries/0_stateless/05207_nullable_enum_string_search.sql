-- The nested value of a NULL slot is unspecified, and for a `Nullable(Enum)` it is usually not a
-- member of the type: `INSERT ... VALUES (NULL)` and the padding of non-joined rows under
-- `join_use_nulls = 1` both leave a raw 0 there. `LIKE`, `ILIKE`, `NOT LIKE`, `match` and the other
-- string searches read the nested column without the null map, so a single NULL row made them throw
-- `UNKNOWN_ELEMENT_OF_ENUM` (`Unexpected value 0 in enum`), while `toString`, `=` and `IN` on the
-- same rows worked. Those slots now hold the enum's default before a function sees them.

DROP TABLE IF EXISTS t_05207;
CREATE TABLE t_05207 (e Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05207 VALUES (NULL), ('a'), ('b');

SELECT DISTINCT 'the NULL slot holds a value that is not in the enum', toInt8(assumeNotNull(e)) FROM t_05207 WHERE e IS NULL;

SELECT 'LIKE', count() FROM t_05207 WHERE e LIKE 'a%';
SELECT 'ILIKE', count() FROM t_05207 WHERE e ILIKE 'A%';
SELECT 'NOT LIKE', count() FROM t_05207 WHERE e NOT LIKE 'a%';
SELECT 'match', count() FROM t_05207 WHERE match(e, '^a');
SELECT 'position', count() FROM t_05207 WHERE position(e, 'a') > 0;
SELECT 'hasToken', count() FROM t_05207 WHERE hasToken(e, 'a');
SELECT 'countSubstrings', sum(countSubstrings(e, 'a')) FROM t_05207;

-- The same answers as the paths that were already null-aware.
SELECT 'toString and LIKE', count() FROM t_05207 WHERE toString(e) LIKE 'a%';
SELECT 'equals', count() FROM t_05207 WHERE e = 'a';
SELECT 'IN', count() FROM t_05207 WHERE e IN ('a');

-- Every row of the whole column, so the NULL row's own result is visible.
SELECT e, e LIKE 'a%', match(e, '^a') FROM t_05207 ORDER BY e NULLS FIRST;

-- Whether the null rows are filtered out before the function runs must not matter.
SELECT 'short circuit off', count() FROM t_05207 WHERE e LIKE 'a%' SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT 'short circuit on', count() FROM t_05207 WHERE e LIKE 'a%'
SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0;

-- `Enum16` stores the code in two bytes and is otherwise the same.
DROP TABLE IF EXISTS t_05207_16;
CREATE TABLE t_05207_16 (e Nullable(Enum16('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05207_16 VALUES (NULL), ('a'), ('b');
SELECT 'Enum16', count() FROM t_05207_16 WHERE e LIKE 'a%';
DROP TABLE t_05207_16;

-- An enum that declares 0 never had the problem, because the value the null slot holds is a member
-- of the type. Normalizing the slot must not change what such a column answers.
DROP TABLE IF EXISTS t_05207_zero;
CREATE TABLE t_05207_zero (e Nullable(Enum8('z' = 0, 'a' = 1))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05207_zero VALUES (NULL), ('z'), ('a');
SELECT 'an enum that declares 0', count() FROM t_05207_zero WHERE e LIKE 'z%';
SELECT e, e LIKE 'z%' FROM t_05207_zero ORDER BY e NULLS FIRST;
DROP TABLE t_05207_zero;

-- A constant NULL of an enum type takes the same path.
SELECT 'a constant NULL', CAST(NULL AS Nullable(Enum8('a' = 1, 'b' = 2))) LIKE 'a%';

-- The padding of non-joined rows is the other producer of such a slot, in every join algorithm.
DROP TABLE IF EXISTS t_05207_left;
DROP TABLE IF EXISTS t_05207_right;
CREATE TABLE t_05207_left (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05207_right (k UInt64, e Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05207_left SELECT number FROM numbers(100);
INSERT INTO t_05207_right SELECT number * 2, if(number % 2 = 0, 'a', 'b') FROM numbers(30);

SELECT 'hash', countIf(r.e LIKE 'a%') FROM t_05207_left AS l LEFT JOIN t_05207_right AS r ON l.k = r.k
SETTINGS join_use_nulls = 1, join_algorithm = 'hash';
SELECT 'parallel_hash', countIf(r.e LIKE 'a%') FROM t_05207_left AS l LEFT JOIN t_05207_right AS r ON l.k = r.k
SETTINGS join_use_nulls = 1, join_algorithm = 'parallel_hash';
SELECT 'full_sorting_merge', countIf(r.e LIKE 'a%') FROM t_05207_left AS l LEFT JOIN t_05207_right AS r ON l.k = r.k
SETTINGS join_use_nulls = 1, join_algorithm = 'full_sorting_merge';
SELECT 'grace_hash', countIf(r.e LIKE 'a%') FROM t_05207_left AS l LEFT JOIN t_05207_right AS r ON l.k = r.k
SETTINGS join_use_nulls = 1, join_algorithm = 'grace_hash';

-- The filter above the join and the same filter pushed below it agree.
SELECT 'above the join', count() FROM t_05207_left AS l LEFT JOIN t_05207_right AS r ON l.k = r.k
WHERE r.e LIKE 'a%' OR l.k = 1 SETTINGS join_use_nulls = 1;

DROP TABLE t_05207_right;
DROP TABLE t_05207_left;
DROP TABLE t_05207;
