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

INSERT INTO t_s
SELECT number,
  arrayMap(j -> ( repeat(substring('abc', 1 + (cityHash64(number, j) % 3), 1),
                         [1, 50, 63, 64, 65, 127, 200][1 + (cityHash64(number, j, 7) % 7)]),
                  substring(hex(cityHash64(number, j, 9)), 1, 1 + (cityHash64(number, j, 11) % 16)) ),
    range(1 + (cityHash64(number) % 5)))::Map(String, String)
FROM numbers(200);
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
