-- Tests that joins on a single LowCardinality key column produce the same results as the
-- equivalent join with the key materialized to its plain type. A non-nullable LowCardinality key
-- now uses a dictionary-aware hash map (low_cardinality_key*), so this guards that the dictionary
-- handling, the conditional key materialization, and the offset-based used-flags are all correct.
-- Each query prints 1 when the LowCardinality result equals the plain-type result for that join
-- kind; the per-row checksum is order-independent.

SET allow_suspicious_low_cardinality_types = 1;
-- The dictionary-aware map is a hash-join feature, and this test uses SEMI/ANTI joins that the
-- merge-based algorithms do not implement, so pin the algorithm (other settings stay randomized).
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS lcj_l_lc;
DROP TABLE IF EXISTS lcj_r_lc;
DROP TABLE IF EXISTS lcj_l_pl;
DROP TABLE IF EXISTS lcj_r_pl;

-- The left side has more distinct keys than the right, so some keys are unmatched on each side
-- (exercises LEFT/RIGHT/FULL/SEMI/ANTI), and every key repeats many times (exercises the build-side
-- append and the probe-side dictionary deduplication).
CREATE TABLE lcj_l_lc (k LowCardinality(String), v UInt64) ENGINE = Memory;
CREATE TABLE lcj_r_lc (k LowCardinality(String), w UInt64) ENGINE = Memory;
CREATE TABLE lcj_l_pl (k String, v UInt64) ENGINE = Memory;
CREATE TABLE lcj_r_pl (k String, w UInt64) ENGINE = Memory;

INSERT INTO lcj_l_lc SELECT toString(number % 50), number FROM numbers(2000);
INSERT INTO lcj_r_lc SELECT toString(number % 40), number FROM numbers(1500);
INSERT INTO lcj_l_pl SELECT toString(number % 50), number FROM numbers(2000);
INSERT INTO lcj_r_pl SELECT toString(number % 40), number FROM numbers(1500);

SELECT 'all_inner',  (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_lc ALL INNER JOIN lcj_r_lc USING (k)) = (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_pl ALL INNER JOIN lcj_r_pl USING (k));
-- ANY picks an unspecified matching row (build-order dependent under parallelism), so only the row
-- count is deterministic; the rigorous value checks are the ALL / SEMI / ANTI cases above and below.
SELECT 'any_inner',  (SELECT count() FROM lcj_l_lc ANY INNER JOIN lcj_r_lc USING (k)) = (SELECT count() FROM lcj_l_pl ANY INNER JOIN lcj_r_pl USING (k));
SELECT 'all_left',   (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_lc ALL LEFT JOIN lcj_r_lc USING (k)) = (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_pl ALL LEFT JOIN lcj_r_pl USING (k));
SELECT 'any_left',   (SELECT count() FROM lcj_l_lc ANY LEFT JOIN lcj_r_lc USING (k)) = (SELECT count() FROM lcj_l_pl ANY LEFT JOIN lcj_r_pl USING (k));
SELECT 'all_right',  (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_lc ALL RIGHT JOIN lcj_r_lc USING (k)) = (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_pl ALL RIGHT JOIN lcj_r_pl USING (k));
SELECT 'all_full',   (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_lc ALL FULL JOIN lcj_r_lc USING (k)) = (SELECT (count(), sum(cityHash64(toString(k))), sum(ifNull(v, 0)), sum(ifNull(w, 0))) FROM lcj_l_pl ALL FULL JOIN lcj_r_pl USING (k));
SELECT 'semi_left',  (SELECT (count(), sum(cityHash64(toString(k))), sum(v)) FROM lcj_l_lc SEMI LEFT JOIN lcj_r_lc USING (k)) = (SELECT (count(), sum(cityHash64(toString(k))), sum(v)) FROM lcj_l_pl SEMI LEFT JOIN lcj_r_pl USING (k));
SELECT 'anti_left',  (SELECT (count(), sum(cityHash64(toString(k))), sum(v)) FROM lcj_l_lc ANTI LEFT JOIN lcj_r_lc USING (k)) = (SELECT (count(), sum(cityHash64(toString(k))), sum(v)) FROM lcj_l_pl ANTI LEFT JOIN lcj_r_pl USING (k));

-- A single FixedString LowCardinality key also uses a dictionary-aware map.
DROP TABLE IF EXISTS lcj_fs_lc;
DROP TABLE IF EXISTS lcj_fs_pl;
CREATE TABLE lcj_fs_lc (k LowCardinality(FixedString(4)), v UInt64) ENGINE = Memory;
CREATE TABLE lcj_fs_pl (k FixedString(4), v UInt64) ENGINE = Memory;
INSERT INTO lcj_fs_lc SELECT toFixedString(leftPad(toString(number % 50), 4, '0'), 4), number FROM numbers(2000);
INSERT INTO lcj_fs_pl SELECT toFixedString(leftPad(toString(number % 50), 4, '0'), 4), number FROM numbers(2000);
SELECT 'fixedstring', (SELECT (count(), sum(l.v + r.v)) FROM lcj_fs_lc AS l ALL INNER JOIN lcj_fs_lc AS r USING (k)) = (SELECT (count(), sum(l.v + r.v)) FROM lcj_fs_pl AS l ALL INNER JOIN lcj_fs_pl AS r USING (k));

-- Numeric LowCardinality keys are intentionally not routed to a dictionary-aware map; they keep the
-- materialized key* path (which retains the FixedHashMap conversion and the runtime filter). This
-- guards that such a key still joins correctly.
DROP TABLE IF EXISTS lcj_u32_lc;
DROP TABLE IF EXISTS lcj_u32_pl;
CREATE TABLE lcj_u32_lc (k LowCardinality(UInt32), v UInt64) ENGINE = Memory;
CREATE TABLE lcj_u32_pl (k UInt32, v UInt64) ENGINE = Memory;
INSERT INTO lcj_u32_lc SELECT number % 50, number FROM numbers(2000);
INSERT INTO lcj_u32_pl SELECT number % 50, number FROM numbers(2000);
SELECT 'uint32', (SELECT (count(), sum(l.v + r.v)) FROM lcj_u32_lc AS l ALL INNER JOIN lcj_u32_lc AS r USING (k)) = (SELECT (count(), sum(l.v + r.v)) FROM lcj_u32_pl AS l ALL INNER JOIN lcj_u32_pl AS r USING (k));

-- LowCardinality(Nullable(T)) keeps the materialized fallback; verify it stays correct.
DROP TABLE IF EXISTS lcj_n_lc;
DROP TABLE IF EXISTS lcj_n_pl;
CREATE TABLE lcj_n_lc (k LowCardinality(Nullable(String)), v UInt64) ENGINE = Memory;
CREATE TABLE lcj_n_pl (k Nullable(String), v UInt64) ENGINE = Memory;
INSERT INTO lcj_n_lc SELECT if(number % 7 = 0, NULL, toString(number % 50)), number FROM numbers(2000);
INSERT INTO lcj_n_pl SELECT if(number % 7 = 0, NULL, toString(number % 50)), number FROM numbers(2000);
SELECT 'nullable_inner', (SELECT (count(), sum(l.v + r.v)) FROM lcj_n_lc AS l ALL INNER JOIN lcj_n_lc AS r USING (k)) = (SELECT (count(), sum(l.v + r.v)) FROM lcj_n_pl AS l ALL INNER JOIN lcj_n_pl AS r USING (k));
SELECT 'nullable_anti', (SELECT (count(), sum(cityHash64(ifNull(k, '<n>'))), sum(l.v)) FROM lcj_n_lc AS l ANTI LEFT JOIN lcj_n_lc AS r USING (k)) = (SELECT (count(), sum(cityHash64(ifNull(k, '<n>'))), sum(l.v)) FROM lcj_n_pl AS l ANTI LEFT JOIN lcj_n_pl AS r USING (k));

DROP TABLE lcj_l_lc;
DROP TABLE lcj_r_lc;
DROP TABLE lcj_l_pl;
DROP TABLE lcj_r_pl;
DROP TABLE lcj_fs_lc;
DROP TABLE lcj_fs_pl;
DROP TABLE lcj_u32_lc;
DROP TABLE lcj_u32_pl;
DROP TABLE lcj_n_lc;
DROP TABLE lcj_n_pl;
