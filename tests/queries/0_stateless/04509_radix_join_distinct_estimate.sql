-- Tags: no-random-settings
-- The `radix_join` per-leaf HLL distinct-key table sizing (radix_join_size_tables_by_distinct_estimate)
-- only affects the internal leaf hash-table sizing, never the join result: `radix_join` must agree with
-- `hash` and be identical with the setting on or off, including on a duplicate-heavy build where the
-- distinct estimate actually shrinks the tables.
-- The old analyzer was intentionally not taught `radix_join`, hence enable_analyzer = 1.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS rhj_de_b;
DROP TABLE IF EXISTS rhj_de_p;

CREATE TABLE rhj_de_b (k UInt64, pay UInt64) ENGINE = Memory;
CREATE TABLE rhj_de_p (k UInt64, pay UInt64) ENGINE = Memory;

-- Duplicate-heavy build: 200 distinct keys over 100000 rows (~500 rows/key) -> the distinct estimate sizes
-- the leaf tables far smaller than the row count, exercising the estimate path.
INSERT INTO rhj_de_b SELECT number % 200, number FROM numbers(100000);
INSERT INTO rhj_de_p SELECT number % 250, number FROM numbers(50000);

-- Each row prints the case name and 1 when the two sides agree on (count, value fingerprint).
SELECT 'dup_on_eq_hash', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 1)
                       = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'dup_off_eq_hash', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 0)
                        = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'dup_on_eq_off', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 1)
                      = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 0);

-- Unique-key build (estimate ~= rows): same agreement.
TRUNCATE TABLE rhj_de_b;
TRUNCATE TABLE rhj_de_p;
INSERT INTO rhj_de_b SELECT number, number FROM numbers(50000);
INSERT INTO rhj_de_p SELECT number * 2, number FROM numbers(50000);

SELECT 'uniq_on_eq_hash', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 1)
                        = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'hash');

SELECT 'uniq_on_eq_off', (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 1)
                       = (SELECT (count(), sum(cityHash64(p.pay, b.pay))) FROM rhj_de_p AS p INNER JOIN rhj_de_b AS b ON p.k = b.k SETTINGS join_algorithm = 'radix_join', radix_join_size_tables_by_distinct_estimate = 0);

DROP TABLE rhj_de_b;
DROP TABLE rhj_de_p;
