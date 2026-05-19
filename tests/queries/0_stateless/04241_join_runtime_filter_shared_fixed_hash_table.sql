-- Tags: no-random-settings

DROP TABLE IF EXISTS phjf_build;
DROP TABLE IF EXISTS phjf_probe;

CREATE TABLE phjf_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE phjf_probe (k Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO phjf_build SELECT number FROM numbers(100);                       -- dense range [0, 99]
INSERT INTO phjf_probe SELECT if(number % 50 = 0, NULL, toInt32(number % 250)) FROM numbers(20000);

SET join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

-- Case 1: INNER JOIN, dense range -> shared filter active when ON.
SELECT 'inner_dense', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'inner_dense', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Case 2: build size = 1 -> guard skips shared publish, baseline `== const` path stays.
SELECT 'inner_size1', count() FROM phjf_probe p INNER JOIN (SELECT 42 AS k) b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'inner_size1', count() FROM phjf_probe p INNER JOIN (SELECT 42 AS k) b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Case 3: LEFT JOIN -> joinRuntimeFilter never fires; results must still match.
SELECT 'left_dense', count(), sum(p.k IS NULL), sum(b.k IS NULL) FROM phjf_probe p LEFT JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'left_dense', count(), sum(p.k IS NULL), sum(b.k IS NULL) FROM phjf_probe p LEFT JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

-- Case 4: range too wide for FixedHashMap conversion -> shared filter not published, Set/BF stays.
DROP TABLE phjf_build;
CREATE TABLE phjf_build (k Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO phjf_build SELECT number * 500000 FROM numbers(100);              -- range 0..49.5M >> 2^18
INSERT INTO phjf_probe SELECT toInt32(number * 500000) FROM numbers(50);       -- guarantee some matches
SELECT 'inner_wide_range', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 0;
SELECT 'inner_wide_range', count() FROM phjf_probe p INNER JOIN phjf_build b ON p.k = b.k SETTINGS enable_join_runtime_filter_shared_fixed_hash_table = 1;

DROP TABLE phjf_build;
DROP TABLE phjf_probe;
