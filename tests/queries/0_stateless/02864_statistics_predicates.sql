-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

-- Tests the cross product of all predicates with all right-hand sides on all data types and all statistics types.

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    u64                 UInt64,
    u64_tdigest         UInt64 STATISTICS(tdigest),
    u64_minmax          UInt64 STATISTICS(minmax),
    u64_countmin       UInt64 STATISTICS(countmin),
    u64_uniq            UInt64 STATISTICS(uniq),
    f64                 Float64,
    f64_tdigest         Float64 STATISTICS(tdigest),
    f64_minmax          Float64 STATISTICS(minmax),
    f64_countmin       Float64 STATISTICS(countmin),
    f64_uniq            Float64 STATISTICS(uniq),
    dt                  DateTime,
    dt_tdigest          DateTime STATISTICS(tdigest),
    dt_minmax           DateTime STATISTICS(minmax),
    dt_countmin        DateTime STATISTICS(countmin),
    dt_uniq             DateTime STATISTICS(uniq),
    b                   Bool,
    b_tdigest           Bool STATISTICS(tdigest),
    b_minmax            Bool STATISTICS(minmax),
    b_countmin         Bool STATISTICS(countmin),
    b_uniq              Bool STATISTICS(uniq),
    s                   String,
 -- s_tdigest           String STATISTICS(tdigest), -- not supported by tdigest
 -- s_minmax            String STATISTICS(minmax), -- not supported by minmax
    s_countmin         String STATISTICS(countmin),
    s_uniq              String STATISTICS(uniq)
) Engine = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO tab
-- SELECT number % 10000, number % 1000, -(number % 100) FROM system.numbers LIMIT 10000;
SELECT number % 1000, -- u64
       number % 1000,
       number % 1000,
       number % 1000,
       number % 1000,
       number % 1000, -- f64
       number % 1000,
       number % 1000,
       number % 1000,
       number % 1000,
       number % 1000, -- dt
       number % 1000,
       number % 1000,
       number % 1000,
       number % 1000,
       number % 2,    -- b
       number % 2,
       number % 2,
       number % 2,
       number % 2,
       toString(number % 1000),
       toString(number % 1000),
       toString(number % 1000)
FROM system.numbers LIMIT 10000;

-- u64 ----------------------------------------------------

SELECT 'u64 and =';

SELECT count(*) FROM tab WHERE u64 = 7;
SELECT count(*) FROM tab WHERE u64_tdigest = 7;
SELECT count(*) FROM tab WHERE u64_minmax = 7;
SELECT count(*) FROM tab WHERE u64_countmin = 7;
SELECT count(*) FROM tab WHERE u64_uniq = 7;

SELECT count(*) FROM tab WHERE u64 = 7.7;
SELECT count(*) FROM tab WHERE u64_tdigest = 7.7;
SELECT count(*) FROM tab WHERE u64_minmax = 7.7;
SELECT count(*) FROM tab WHERE u64_countmin = 7.7;
SELECT count(*) FROM tab WHERE u64_uniq = 7.7;

SELECT count(*) FROM tab WHERE u64 = '7';
SELECT count(*) FROM tab WHERE u64_tdigest = '7';
SELECT count(*) FROM tab WHERE u64_minmax = '7';
SELECT count(*) FROM tab WHERE u64_countmin = '7';
SELECT count(*) FROM tab WHERE u64_uniq = '7';

SELECT count(*) FROM tab WHERE u64 = '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_tdigest = '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_minmax = '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_countmin = '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_uniq = '7.7'; -- { serverError TYPE_MISMATCH }

SELECT 'u64 and <';

SELECT count(*) FROM tab WHERE u64 < 7;
SELECT count(*) FROM tab WHERE u64_tdigest < 7;
SELECT count(*) FROM tab WHERE u64_minmax < 7;
SELECT count(*) FROM tab WHERE u64_countmin < 7;
SELECT count(*) FROM tab WHERE u64_uniq < 7;

SELECT count(*) FROM tab WHERE u64 < 7.7;
SELECT count(*) FROM tab WHERE u64_tdigest < 7.7;
SELECT count(*) FROM tab WHERE u64_minmax < 7.7;
SELECT count(*) FROM tab WHERE u64_countmin < 7.7;
SELECT count(*) FROM tab WHERE u64_uniq < 7.7;

SELECT count(*) FROM tab WHERE u64 < '7';
SELECT count(*) FROM tab WHERE u64_tdigest < '7';
SELECT count(*) FROM tab WHERE u64_minmax < '7';
SELECT count(*) FROM tab WHERE u64_countmin < '7';
SELECT count(*) FROM tab WHERE u64_uniq < '7';

SELECT count(*) FROM tab WHERE u64 < '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_tdigest < '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_minmax < '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_countmin < '7.7'; -- { serverError TYPE_MISMATCH }
SELECT count(*) FROM tab WHERE u64_uniq < '7.7'; -- { serverError TYPE_MISMATCH }

-- f64 ----------------------------------------------------

SELECT 'f64 and =';

SELECT count(*) FROM tab WHERE f64 = 7;
SELECT count(*) FROM tab WHERE f64_tdigest = 7;
SELECT count(*) FROM tab WHERE f64_minmax = 7;
SELECT count(*) FROM tab WHERE f64_countmin = 7;
SELECT count(*) FROM tab WHERE f64_uniq = 7;

SELECT count(*) FROM tab WHERE f64 = 7.7;
SELECT count(*) FROM tab WHERE f64_tdigest = 7.7;
SELECT count(*) FROM tab WHERE f64_minmax = 7.7;
SELECT count(*) FROM tab WHERE f64_countmin = 7.7;
SELECT count(*) FROM tab WHERE f64_uniq = 7.7;

SELECT count(*) FROM tab WHERE f64 = '7';
SELECT count(*) FROM tab WHERE f64_tdigest = '7';
SELECT count(*) FROM tab WHERE f64_minmax = '7';
SELECT count(*) FROM tab WHERE f64_countmin = '7';
SELECT count(*) FROM tab WHERE f64_uniq = '7';

SELECT count(*) FROM tab WHERE f64 = '7.7';
SELECT count(*) FROM tab WHERE f64_tdigest = '7.7';
SELECT count(*) FROM tab WHERE f64_minmax = '7.7';
SELECT count(*) FROM tab WHERE f64_countmin = '7.7';
SELECT count(*) FROM tab WHERE f64_uniq = '7.7';

SELECT 'f64 and <';

SELECT count(*) FROM tab WHERE f64 < 7;
SELECT count(*) FROM tab WHERE f64_tdigest < 7;
SELECT count(*) FROM tab WHERE f64_minmax < 7;
SELECT count(*) FROM tab WHERE f64_countmin < 7;
SELECT count(*) FROM tab WHERE f64_uniq < 7;

SELECT count(*) FROM tab WHERE f64 < 7.7;
SELECT count(*) FROM tab WHERE f64_tdigest < 7.7;
SELECT count(*) FROM tab WHERE f64_minmax < 7.7;
SELECT count(*) FROM tab WHERE f64_countmin < 7.7;
SELECT count(*) FROM tab WHERE f64_uniq < 7.7;

SELECT count(*) FROM tab WHERE f64 < '7';
SELECT count(*) FROM tab WHERE f64_tdigest < '7';
SELECT count(*) FROM tab WHERE f64_minmax < '7';
SELECT count(*) FROM tab WHERE f64_countmin < '7';
SELECT count(*) FROM tab WHERE f64_uniq < '7';

SELECT count(*) FROM tab WHERE f64 < '7.7';
SELECT count(*) FROM tab WHERE f64_tdigest < '7.7';
SELECT count(*) FROM tab WHERE f64_minmax < '7.7';
SELECT count(*) FROM tab WHERE f64_countmin < '7.7';
SELECT count(*) FROM tab WHERE f64_uniq < '7.7';

-- dt ----------------------------------------------------

SELECT 'dt and =';

SELECT count(*) FROM tab WHERE dt = '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_tdigest = '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_minmax = '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_countmin = '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_uniq = '2024-08-08 11:12:13';

SELECT count(*) FROM tab WHERE dt = 7;
SELECT count(*) FROM tab WHERE dt_tdigest = 7;
SELECT count(*) FROM tab WHERE dt_minmax = 7;
SELECT count(*) FROM tab WHERE dt_countmin = 7;
SELECT count(*) FROM tab WHERE dt_uniq = 7;

SELECT 'dt and <';

SELECT count(*) FROM tab WHERE dt < '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_tdigest < '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_minmax < '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_countmin < '2024-08-08 11:12:13';
SELECT count(*) FROM tab WHERE dt_uniq < '2024-08-08 11:12:13';

SELECT count(*) FROM tab WHERE dt < 7;
SELECT count(*) FROM tab WHERE dt_tdigest < 7;
SELECT count(*) FROM tab WHERE dt_minmax < 7;
SELECT count(*) FROM tab WHERE dt_countmin < 7;
SELECT count(*) FROM tab WHERE dt_uniq < 7;

-- b ----------------------------------------------------

SELECT 'b and =';

SELECT count(*) FROM tab WHERE b = true;
SELECT count(*) FROM tab WHERE b_tdigest = true;
SELECT count(*) FROM tab WHERE b_minmax = true;
SELECT count(*) FROM tab WHERE b_countmin = true;
SELECT count(*) FROM tab WHERE b_uniq = true;

SELECT count(*) FROM tab WHERE b = 'true';
SELECT count(*) FROM tab WHERE b_tdigest = 'true';
SELECT count(*) FROM tab WHERE b_minmax = 'true';
SELECT count(*) FROM tab WHERE b_countmin = 'true';
SELECT count(*) FROM tab WHERE b_uniq = 'true';

SELECT count(*) FROM tab WHERE b = 1;
SELECT count(*) FROM tab WHERE b_tdigest = 1;
SELECT count(*) FROM tab WHERE b_minmax = 1;
SELECT count(*) FROM tab WHERE b_countmin = 1;
SELECT count(*) FROM tab WHERE b_uniq = 1;

SELECT count(*) FROM tab WHERE b = 1.1;
SELECT count(*) FROM tab WHERE b_tdigest = 1.1;
SELECT count(*) FROM tab WHERE b_minmax = 1.1;
SELECT count(*) FROM tab WHERE b_countmin = 1.1;
SELECT count(*) FROM tab WHERE b_uniq = 1.1;

-- s ----------------------------------------------------

SELECT 's and =';

SELECT count(*) FROM tab WHERE s = 7; -- { serverError NO_COMMON_TYPE }
-- SELECT count(*) FROM tab WHERE s_tdigest = 7; -- not supported
-- SELECT count(*) FROM tab WHERE s_minmax = 7; -- not supported
SELECT count(*) FROM tab WHERE s_countmin = 7; -- { serverError NO_COMMON_TYPE }
SELECT count(*) FROM tab WHERE s_uniq = 7; -- { serverError NO_COMMON_TYPE }

SELECT count(*) FROM tab WHERE s = '7';
-- SELECT count(*) FROM tab WHERE s_tdigest = '7'; -- not supported
-- SELECT count(*) FROM tab WHERE s_minmax = '7'; -- not supported
SELECT count(*) FROM tab WHERE s_countmin = '7';
SELECT count(*) FROM tab WHERE s_uniq = '7';

DROP TABLE tab;
