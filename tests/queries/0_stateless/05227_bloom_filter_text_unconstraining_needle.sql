-- Needles that tokenize to nothing cannot reject a granule. Skipping their granule probes must leave
-- both the results and the granule selection of every atom kind exactly as they are.

DROP TABLE IF EXISTS tok;
DROP TABLE IF EXISTS ngr;
DROP TABLE IF EXISTS plain;
DROP TABLE IF EXISTS arr;
DROP TABLE IF EXISTS arr_plain;

CREATE TABLE tok (s String, INDEX idx s TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
CREATE TABLE ngr (s String, INDEX idx s TYPE ngrambf_v1(3, 8192, 3, 0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
CREATE TABLE plain (s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

CREATE TABLE arr (a Array(String), INDEX idx a TYPE tokenbf_v1(8192, 3, 0) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;
CREATE TABLE arr_plain (a Array(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 4;

-- 32 rows, 8 granules. 'bb', 'ee', 'realtoken' and the 3-gram 'abc' each occur in exactly one row;
-- 'google' occurs in three; 'zz' occurs nowhere.
INSERT INTO plain VALUES ('aa bb cc'), ('dd ee ff'), ('google alpha'), ('gamma google'),
    ('realtoken here'), ('abcdef ghi'), ('google omega'), ('q7 w7 r7'),
    ('q8 w8 r8'), ('q9 w9 r9'), ('q10 w10 r10'), ('q11 w11 r11'),
    ('q12 w12 r12'), ('q13 w13 r13'), ('q14 w14 r14'), ('q15 w15 r15'),
    ('q16 w16 r16'), ('q17 w17 r17'), ('q18 w18 r18'), ('q19 w19 r19'),
    ('q20 w20 r20'), ('q21 w21 r21'), ('q22 w22 r22'), ('q23 w23 r23'),
    ('q24 w24 r24'), ('q25 w25 r25'), ('q26 w26 r26'), ('q27 w27 r27'),
    ('q28 w28 r28'), ('q29 w29 r29'), ('q30 w30 r30'), ('q31 w31 r31');

INSERT INTO tok SELECT s FROM plain;
INSERT INTO ngr SELECT s FROM plain;

INSERT INTO arr_plain VALUES (['realtoken', '']), (['a1', 'b1']), (['a2', 'b2']), (['a3', 'b3']),
    (['a4', 'b4']), (['a5', 'b5']), (['a6', 'b6']), (['a7', 'b7']);
INSERT INTO arr SELECT a FROM arr_plain;

SELECT 'a: an unconstraining needle keeps the results and keeps the index in use';
SELECT (SELECT count() FROM tok WHERE multiSearchAny(s, ['google']))
     = (SELECT count() FROM plain WHERE multiSearchAny(s, ['google'])) AS same_rows,
       (SELECT count() FROM plain WHERE multiSearchAny(s, ['google'])) > 0 AS rows_returned;
SELECT count() > 0 AS skip_index_still_reported
FROM (EXPLAIN indexes = 1 SELECT count() FROM tok WHERE multiSearchAny(s, ['google']))
WHERE explain ILIKE '%Name: idx%';
SELECT sel = tot AS all_granules_kept FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM tok WHERE multiSearchAny(s, ['google']))
    WHERE explain ILIKE '%Granules: %/%'
);

SELECT 'b: a needle with a surviving token still prunes';
SELECT sel < tot AS prunes FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM tok WHERE multiSearchAny(s, ['aa bb cc']))
    WHERE explain ILIKE '%Granules: %/%'
);

SELECT 'c: hasAll keeps pruning on its constraining needles';
SELECT sel < tot AS prunes FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM arr WHERE hasAll(a, ['', 'realtoken']))
    WHERE explain ILIKE '%Granules: %/%'
);
SELECT (SELECT count() FROM arr WHERE hasAll(a, ['', 'realtoken']))
     = (SELECT count() FROM arr_plain WHERE hasAll(a, ['', 'realtoken'])) AS same_rows,
       (SELECT count() FROM arr_plain WHERE hasAll(a, ['', 'realtoken'])) > 0 AS rows_returned;

SELECT 'd: an unconstraining needle inside a disjunction of needles must not be dropped';
SELECT (SELECT count() FROM tok WHERE multiSearchAny(s, ['google', 'aa zz cc']))
     = (SELECT count() FROM plain WHERE multiSearchAny(s, ['google', 'aa zz cc'])) AS same_rows,
       (SELECT count() FROM plain WHERE multiSearchAny(s, ['google', 'aa zz cc'])) > 0 AS rows_returned;

SELECT 'e: match keeps pruning through both the required substring and the alternatives';
SELECT sel < tot AS prunes FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM tok WHERE match(s, 'aa bb cc'))
    WHERE explain ILIKE '%Granules: %/%'
);
SELECT sel < tot AS prunes FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM tok WHERE match(s, 'aa bb cc|dd ee ff'))
    WHERE explain ILIKE '%Granules: %/%'
);

SELECT 'f: a needle shorter than the ngram length is unconstraining, a long enough one still prunes';
SELECT (SELECT count() FROM ngr WHERE multiSearchAny(s, ['ab']))
     = (SELECT count() FROM plain WHERE multiSearchAny(s, ['ab'])) AS same_rows,
       (SELECT count() FROM plain WHERE multiSearchAny(s, ['ab'])) > 0 AS rows_returned;
SELECT sel < tot AS prunes FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM ngr WHERE multiSearchAny(s, ['abc']))
    WHERE explain ILIKE '%Granules: %/%'
);

SELECT 'g: an IN set row that tokenizes to nothing keeps the results, a constraining set still prunes';
SELECT (SELECT count() FROM tok WHERE s IN ('', 'realtoken here'))
     = (SELECT count() FROM plain WHERE s IN ('', 'realtoken here')) AS same_rows,
       (SELECT count() FROM plain WHERE s IN ('', 'realtoken here')) > 0 AS rows_returned;
SELECT sel < tot AS prunes FROM
(
    SELECT toUInt64(extract(explain, 'Granules: (\\d+)/')) AS sel,
           toUInt64(extract(explain, 'Granules: \\d+/(\\d+)')) AS tot
    FROM (EXPLAIN indexes = 1 SELECT count() FROM tok WHERE s IN ('realtoken here', 'aa bb cc'))
    WHERE explain ILIKE '%Granules: %/%'
);

SELECT 'h: an unconstraining needle under NOT is may-be-true, not true';
SELECT (SELECT count() FROM tok WHERE NOT multiSearchAny(s, ['google']))
     = (SELECT count() FROM plain WHERE NOT multiSearchAny(s, ['google'])) AS same_rows,
       (SELECT count() FROM plain WHERE NOT multiSearchAny(s, ['google'])) > 0 AS rows_returned;

SELECT 'i: an index carrying only unconstraining atoms is still a used index';
SELECT count() FROM tok WHERE multiSearchAny(s, ['google']) SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE tok;
DROP TABLE ngr;
DROP TABLE plain;
DROP TABLE arr;
DROP TABLE arr_plain;
