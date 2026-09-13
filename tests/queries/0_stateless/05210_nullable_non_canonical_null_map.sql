-- { echo }
-- Random settings limits: compile_expressions=(0, 0); ratio_of_defaults_for_sparse_serialization=(1.0, 1.0)
-- A null-map byte is a predicate, not a value: any non-zero byte means NULL. `if` passes its raw
-- condition column on as the null map, so `number % 3` fills it with the bytes 0, 1 and 2. Byte 0
-- marks the rows holding 'x'; the 1s and the 2s are equally NULL. Every reader below must agree
-- with `isNullAt` and treat 1 and 2 alike.
-- The aggregates read the DISTINCT result from the outside: an `ORDER BY` on the DISTINCT itself
-- would sort the keys and answer from `compareAt`, which is nullness-based already.
SELECT count(), countIf(e IS NULL), arraySort(groupUniqArray(e)) FROM (SELECT DISTINCT if(number % 3, NULL, 'x') AS e FROM numbers(30));
SELECT 1 AS k, if(number % 3, NULL, 'x') AS e FROM numbers(30) GROUP BY k, e ORDER BY e NULLS FIRST;
-- Multiplying the condition by 2 leaves no byte equal to 1 in the null map. A reader that scans for
-- the literal byte 1 then reports the block as NULL-free and merges the NULL rows into the value.
SELECT e FROM (SELECT if(toUInt8((number % 3) * 2), NULL, 'x') AS e FROM numbers(30)) GROUP BY e ORDER BY e NULLS FIRST;
SELECT DISTINCT if(toUInt8((number % 3) * 2), NULL, toUInt32(7)) FROM numbers(30) ORDER BY 1 NULLS FIRST;
SELECT DISTINCT tuple(if(number % 3, NULL, 'x')) FROM numbers(30) ORDER BY 1;
-- Negating a null map must negate its nullness, not its bits: `1 ^ 2` is truthy, which turned rows
-- holding 'x' into NULLs. Only the group whose condition byte is 2 was affected.
SELECT number % 3 AS c, countIf(v IS NULL) FROM (SELECT number, if(number % 3, toNullable('x'), NULL) AS v FROM numbers(30)) GROUP BY c ORDER BY c;
SELECT count(), countIf(e IS NULL), arraySort(groupUniqArray(e)) FROM (SELECT DISTINCT if(number % 3, NULL, CAST('x' AS LowCardinality(Nullable(String)))) AS e FROM numbers(30));
-- A NULL condition selects no branch. Only a three-argument `multiIf` is rewritten to `if`
-- (`MultiIfToIfPass`), so the five arguments here keep it on `multiIf`'s own columnar path.
SELECT r, count() FROM (SELECT multiIf(if(number % 3, NULL, toUInt8(1)), toUInt32(11), number % 7 = 0, toUInt32(33), toUInt32(22)) AS r FROM numbers(30)) GROUP BY r ORDER BY r;
-- An aggregate whose result is Nullable must return NULL for a block in which every row is NULL. The
-- bytes here are 2, 4 and 6, so a reader looking for the literal byte 1 finds none, calls the block
-- non-NULL, and emits the nested aggregate's never-updated state.
SELECT max(if(toUInt8((number % 3) * 2 + 2), NULL, 'x')) AS m, m IS NULL AS m_is_null FROM numbers(30);
-- Two non-empty variants are needed to reach the general Variant null-map branch; with a single
-- variant and no existing NULLs a separate fast path applies, which is nullness-based already.
SELECT countIf(v IS NULL) FROM (SELECT if(number % 3, if(number % 2, 'x', toUInt8(7)), NULL) AS v FROM numbers(30));
-- Only a NULL that is NEW in the converted element is a conversion failure. 1000 overflows UInt8, so
-- the byte-0 rows are the only failures; a source NULL must stay a tuple holding NULL, not a NULL
-- tuple. The target element type must differ from the source, or the result map IS the source map.
SELECT countIf(tp IS NULL) FROM (SELECT accurateCastOrNull(tuple(if(number % 3, NULL, toUInt16(1000))), 'Tuple(Nullable(UInt8))') AS tp FROM numbers(30));
-- Such a null map survives a write and read back, so it is not confined to expression results.
CREATE TABLE t_null_map_bytes (e Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null_map_bytes SELECT if(number % 3, NULL, 'x') FROM numbers(30);
SELECT count(), countIf(e IS NULL), arraySort(groupUniqArray(e)) FROM (SELECT DISTINCT e FROM t_null_map_bytes);
DROP TABLE t_null_map_bytes;
-- A DIRECT layout reads its source rows on demand and passes them through, so the stored null map
-- reaches the reverse lookup's own comparison result. A NULL attribute must not match a value. The
-- caching layouts rebuild the map, which is why only DIRECT exposes this.
CREATE TABLE t_null_map_dict_src (id UInt64, attr Nullable(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_null_map_dict_src SELECT number, if(number % 3, NULL, 'x') FROM numbers(30);
CREATE DICTIONARY d_null_map_direct (id UInt64, attr Nullable(String)) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 't_null_map_dict_src')) LAYOUT(DIRECT());
SELECT length(dictGetKeys('d_null_map_direct', 'attr', 'x'));
DROP DICTIONARY d_null_map_direct;
DROP TABLE t_null_map_dict_src;
-- Controls: a canonical 0/1 null map must be unaffected.
SELECT count(), countIf(e IS NULL), arraySort(groupUniqArray(e)) FROM (SELECT DISTINCT if(number % 2, NULL, 'x') AS e FROM numbers(30));
SELECT number % 2 AS c, countIf(v IS NULL) FROM (SELECT number, if(number % 2, toNullable('x'), NULL) AS v FROM numbers(30)) GROUP BY c ORDER BY c;
-- The reported query: DISTINCT and GROUP BY over one projection must agree.
SELECT
    (SELECT count() FROM (SELECT DISTINCT multiIf(number % -1, -9223372036854775808 < moduloLegacy(number, NULL), '^$') FROM numbers(150))) AS distinct_rows,
    (SELECT count() FROM (SELECT multiIf(number % -1, -9223372036854775808 < moduloLegacy(number, NULL), '^$') AS e FROM numbers(150) GROUP BY e)) AS group_by_rows;
