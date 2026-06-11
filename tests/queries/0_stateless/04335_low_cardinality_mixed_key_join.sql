-- A join with a plain probe key against a LowCardinality build key selects the dictionary-aware map
-- (the map is chosen from the build key, which is LowCardinality), while the probe column stays plain.
-- Joins allow plain T vs LowCardinality(T) without a cast, so the dictionary-aware key getter must
-- handle a plain probe column. The old analyzer keeps the mixed key types; the new analyzer casts
-- them to a common type first, so pin the old analyzer to actually exercise this path. Results must
-- match the all-plain join. Regression for a LOGICAL_ERROR ("Expected LowCardinality column").

SET enable_analyzer = 0;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS lcm_probe;
DROP TABLE IF EXISTS lcm_build_lc;
DROP TABLE IF EXISTS lcm_build_pl;

CREATE TABLE lcm_probe (k String, v UInt64) ENGINE = Memory;
CREATE TABLE lcm_build_lc (k LowCardinality(String), w UInt64) ENGINE = Memory;
CREATE TABLE lcm_build_pl (k String, w UInt64) ENGINE = Memory;

-- The probe has more distinct keys than the build, so some keys are unmatched on each side.
INSERT INTO lcm_probe SELECT toString(number % 50), number FROM numbers(2000);
INSERT INTO lcm_build_lc SELECT toString(number % 40), number FROM numbers(1500);
INSERT INTO lcm_build_pl SELECT toString(number % 40), number FROM numbers(1500);

SELECT 'inner', (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL INNER JOIN lcm_build_lc AS r ON l.k = r.k) = (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL INNER JOIN lcm_build_pl AS r ON l.k = r.k);
SELECT 'left',  (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL LEFT  JOIN lcm_build_lc AS r ON l.k = r.k) = (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL LEFT  JOIN lcm_build_pl AS r ON l.k = r.k);
SELECT 'right', (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL RIGHT JOIN lcm_build_lc AS r ON l.k = r.k) = (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL RIGHT JOIN lcm_build_pl AS r ON l.k = r.k);
SELECT 'full',  (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL FULL  JOIN lcm_build_lc AS r ON l.k = r.k) = (SELECT (count(), sum(ifNull(l.v, 0)), sum(ifNull(r.w, 0))) FROM lcm_probe AS l ALL FULL  JOIN lcm_build_pl AS r ON l.k = r.k);
SELECT 'semi',  (SELECT (count(), sum(l.v)) FROM lcm_probe AS l SEMI LEFT JOIN lcm_build_lc AS r ON l.k = r.k) = (SELECT (count(), sum(l.v)) FROM lcm_probe AS l SEMI LEFT JOIN lcm_build_pl AS r ON l.k = r.k);
SELECT 'anti',  (SELECT (count(), sum(l.v)) FROM lcm_probe AS l ANTI LEFT JOIN lcm_build_lc AS r ON l.k = r.k) = (SELECT (count(), sum(l.v)) FROM lcm_probe AS l ANTI LEFT JOIN lcm_build_pl AS r ON l.k = r.k);

DROP TABLE lcm_probe;
DROP TABLE lcm_build_lc;
DROP TABLE lcm_build_pl;
