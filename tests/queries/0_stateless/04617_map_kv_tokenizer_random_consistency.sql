-- Randomized consistency check for the keyValuePairs text index: the index must never change query
-- results. The same table is queried twice per predicate — once with the index enabled and once with
-- it disabled (use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0, i.e. a brute-force
-- scan = ground truth) — and the two matched-row checksums must agree. Data is deterministic-random
-- with a small key universe (so keys repeat within a row), key/value lengths spanning the 63/64-byte
-- single/multi-byte trailer boundary, and both String and LowCardinality(String) maps.

DROP TABLE IF EXISTS t_s;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS r;

CREATE TABLE t_s (id UInt64, m Map(String, String),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8;
CREATE TABLE t_lc (id UInt64, m Map(LowCardinality(String), LowCardinality(String)),
    INDEX idx m TYPE text(tokenizer = 'keyValuePairs') GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8;

-- Keys and values are arbitrary byte strings (any of 0..255, including NUL, 0x80+, and bytes that
-- look like trailer values) to stress the length-delimited encode/decode. Duplicate keys are built
-- explicitly at the array level: per row we generate a set of base keys, then append a duplicated
-- slice of them and attach fresh values, so every row has a repeated key. Key lengths span the
-- 63/64-byte trailer boundary; ~1/6 of keys and values are the empty string.
INSERT INTO t_s
SELECT number,
  arrayZip(
    all_keys,
    arrayMap(j -> if(cityHash64(number, j, 17) % 6 = 0, '',
                     arrayStringConcat(arrayMap(i -> char(cityHash64(number, j, i, 3) % 256),
                                                range(1 + (cityHash64(number, j, 11) % 20))))),
             range(length(all_keys)))
  )::Map(String, String) AS m
FROM (
  SELECT number, base_keys,
    arrayConcat(base_keys, arraySlice(base_keys, 1, 1 + (cityHash64(number, 5) % length(base_keys)))) AS all_keys
  FROM (
    SELECT number,
      arrayMap(k -> if(cityHash64(number, k, 13) % 6 = 0, '',
                       arrayStringConcat(arrayMap(i -> char(cityHash64(number, k, i) % 256),
                                                  range([1, 50, 63, 64, 65, 127, 200][1 + (cityHash64(number, k, 7) % 7)])))),
               range(1 + (cityHash64(number) % 4))) AS base_keys
    FROM numbers(200)
  )
);
-- Crafted edge rows: guarantee empty-key/empty-value coverage and explicit weird bytes, with dups.
INSERT INTO t_s VALUES
    (100000, map('', 'a', '', 'b')),                                              -- duplicate empty key
    (100001, map('k', '', 'k', 'v')),                                             -- duplicate key, first value empty
    (100002, map('', '', '', '')),                                                -- all empty, duplicated
    (100003, map('x', '', '', 'y')),                                              -- empty value and empty key mixed
    (100004, map(char(0, 128, 255), char(255, 0, 1), char(0, 128, 255), char(2))),-- duplicate NUL/high-byte key
    (100005, map(char(1), char(0), char(200, 0, 50), char(0, 0)));                -- values with leading/embedded NUL
INSERT INTO t_lc SELECT id, CAST(m, 'Map(LowCardinality(String), LowCardinality(String))') FROM t_s;

CREATE TABLE r (q String, use_index UInt8, h UInt64) ENGINE = Memory;
INSERT INTO r SELECT 'ck_s', 1, sipHash64(k, id) FROM t_s, (SELECT DISTINCT arrayJoin(mapKeys(m)) AS k FROM t_s) ks WHERE mapContainsKey(m, k) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ck_s', 0, sipHash64(k, id) FROM t_s, (SELECT DISTINCT arrayJoin(mapKeys(m)) AS k FROM t_s) ks WHERE mapContainsKey(m, k) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'cv_s', 1, sipHash64(v, id) FROM t_s, (SELECT DISTINCT arrayJoin(mapValues(m)) AS v FROM t_s) vs WHERE mapContainsValue(m, v) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'cv_s', 0, sipHash64(v, id) FROM t_s, (SELECT DISTINCT arrayJoin(mapValues(m)) AS v FROM t_s) vs WHERE mapContainsValue(m, v) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ckv_s', 1, sipHash64(p.1, p.2, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE mapContainsKeyValue(m, p.1, p.2) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ckv_s', 0, sipHash64(p.1, p.2, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE mapContainsKeyValue(m, p.1, p.2) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ckl_s', 1, sipHash64(pat, id) FROM t_s, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_s) kp WHERE mapContainsKeyLike(m, pat) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ckl_s', 0, sipHash64(pat, id) FROM t_s, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_s) kp WHERE mapContainsKeyLike(m, pat) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'cvl_s', 1, sipHash64(pat, id) FROM t_s, (SELECT DISTINCT (substring(arrayJoin(mapValues(m)), 1, 2) || '%') AS pat FROM t_s) vp WHERE mapContainsValueLike(m, pat) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'cvl_s', 0, sipHash64(pat, id) FROM t_s, (SELECT DISTINCT (substring(arrayJoin(mapValues(m)), 1, 2) || '%') AS pat FROM t_s) vp WHERE mapContainsValueLike(m, pat) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ckvl_s', 1, sipHash64(pat, id) FROM t_s, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_s) kp WHERE mapContainsKeyValueLike(m, pat, '%') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ckvl_s', 0, sipHash64(pat, id) FROM t_s, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_s) kp WHERE mapContainsKeyValueLike(m, pat, '%') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'eq_s', 1, sipHash64(p.1, p.2, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE m[p.1] = p.2 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'eq_s', 0, sipHash64(p.1, p.2, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE m[p.1] = p.2 SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'el_s', 1, sipHash64(p.1, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE m[p.1] LIKE (p.2 || '%') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'el_s', 0, sipHash64(p.1, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE m[p.1] LIKE (p.2 || '%') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'esw_s', 1, sipHash64(p.1, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE startsWith(m[p.1], substring(p.2, 1, 2)) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'esw_s', 0, sipHash64(p.1, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE startsWith(m[p.1], substring(p.2, 1, 2)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'eew_s', 1, sipHash64(p.1, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE endsWith(m[p.1], substring(p.2, greatest(length(p.2) - 1, 1))) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'eew_s', 0, sipHash64(p.1, id) FROM t_s, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_s) ps WHERE endsWith(m[p.1], substring(p.2, greatest(length(p.2) - 1, 1))) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ck_lc', 1, sipHash64(k, id) FROM t_lc, (SELECT DISTINCT arrayJoin(mapKeys(m)) AS k FROM t_lc) ks WHERE mapContainsKey(m, k) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ck_lc', 0, sipHash64(k, id) FROM t_lc, (SELECT DISTINCT arrayJoin(mapKeys(m)) AS k FROM t_lc) ks WHERE mapContainsKey(m, k) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'cv_lc', 1, sipHash64(v, id) FROM t_lc, (SELECT DISTINCT arrayJoin(mapValues(m)) AS v FROM t_lc) vs WHERE mapContainsValue(m, v) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'cv_lc', 0, sipHash64(v, id) FROM t_lc, (SELECT DISTINCT arrayJoin(mapValues(m)) AS v FROM t_lc) vs WHERE mapContainsValue(m, v) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ckv_lc', 1, sipHash64(p.1, p.2, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE mapContainsKeyValue(m, p.1, p.2) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ckv_lc', 0, sipHash64(p.1, p.2, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE mapContainsKeyValue(m, p.1, p.2) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ckl_lc', 1, sipHash64(pat, id) FROM t_lc, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_lc) kp WHERE mapContainsKeyLike(m, pat) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ckl_lc', 0, sipHash64(pat, id) FROM t_lc, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_lc) kp WHERE mapContainsKeyLike(m, pat) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'cvl_lc', 1, sipHash64(pat, id) FROM t_lc, (SELECT DISTINCT (substring(arrayJoin(mapValues(m)), 1, 2) || '%') AS pat FROM t_lc) vp WHERE mapContainsValueLike(m, pat) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'cvl_lc', 0, sipHash64(pat, id) FROM t_lc, (SELECT DISTINCT (substring(arrayJoin(mapValues(m)), 1, 2) || '%') AS pat FROM t_lc) vp WHERE mapContainsValueLike(m, pat) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'ckvl_lc', 1, sipHash64(pat, id) FROM t_lc, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_lc) kp WHERE mapContainsKeyValueLike(m, pat, '%') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'ckvl_lc', 0, sipHash64(pat, id) FROM t_lc, (SELECT DISTINCT ('%' || substring(arrayJoin(mapKeys(m)), 1, 3) || '%') AS pat FROM t_lc) kp WHERE mapContainsKeyValueLike(m, pat, '%') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'eq_lc', 1, sipHash64(p.1, p.2, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE m[p.1] = p.2 SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'eq_lc', 0, sipHash64(p.1, p.2, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE m[p.1] = p.2 SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'el_lc', 1, sipHash64(p.1, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE m[p.1] LIKE (p.2 || '%') SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'el_lc', 0, sipHash64(p.1, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE m[p.1] LIKE (p.2 || '%') SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'esw_lc', 1, sipHash64(p.1, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE startsWith(m[p.1], substring(p.2, 1, 2)) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'esw_lc', 0, sipHash64(p.1, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE startsWith(m[p.1], substring(p.2, 1, 2)) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;
INSERT INTO r SELECT 'eew_lc', 1, sipHash64(p.1, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE endsWith(m[p.1], substring(p.2, greatest(length(p.2) - 1, 1))) SETTINGS use_skip_indexes = 1, query_plan_direct_read_from_text_index = 1, use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1;
INSERT INTO r SELECT 'eew_lc', 0, sipHash64(p.1, id) FROM t_lc, (SELECT DISTINCT arrayJoin(arrayZip(mapKeys(m), mapValues(m))) AS p FROM t_lc) ps WHERE endsWith(m[p.1], substring(p.2, greatest(length(p.2) - 1, 1))) SETTINGS use_skip_indexes = 0, query_plan_direct_read_from_text_index = 0;

SELECT 'directread_used', count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_s WHERE mapContainsKey(m, repeat('a', 64)) SETTINGS query_plan_direct_read_from_text_index = 1) WHERE explain LIKE '%__text_index_%';
SELECT 'probes_matched', (SELECT count() FROM r WHERE use_index = 1) > 0;

SELECT q, sumIf(h, use_index = 1) AS with_index, sumIf(h, use_index = 0) AS without_index
FROM r GROUP BY q HAVING with_index != without_index ORDER BY q;

DROP TABLE t_s;
DROP TABLE t_lc;
DROP TABLE r;
