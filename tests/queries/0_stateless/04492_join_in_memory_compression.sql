-- Tests for `enable_join_in_memory_compression`: hash-based joins compress their stored right-side
-- blocks under memory pressure (instead of only shrinking), then decompress them at probe time.

DROP TABLE IF EXISTS jimc_left;
DROP TABLE IF EXISTS jimc_right;

CREATE TABLE jimc_left (k UInt64, lv UInt64) ENGINE = Memory;
CREATE TABLE jimc_right (k UInt64, rv UInt64, pad String) ENGINE = Memory;

-- Unique keys (so ANY picks the only match deterministically) and a highly compressible padding column
-- on the right (build) side. The checksums combine left and right columns, which forces `pad` through
-- the join (it cannot be projected away below the join) so it is actually stored and compressed.
INSERT INTO jimc_left SELECT number, number FROM numbers(40000);
INSERT INTO jimc_right SELECT number, number, repeat('x', 1000) FROM numbers(40000);

-- The uncompressed right table (~46 MiB) is well above `max_bytes_in_join`, so without compression the
-- build overflows the limit and throws ...
SELECT sum(cityHash64(l.lv, r.pad)) FROM jimc_left AS l INNER JOIN jimc_right AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 24000000, join_overflow_mode = 'throw', query_plan_join_swap_table = 'false'; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- ... while with compression it shrinks well below the limit and the query succeeds. The result must
-- equal the uncompressed one (left subquery: no compression, no limit; right subquery: forced
-- compression with a limit that would otherwise overflow), for every join kind and strictness.

SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l INNER JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l INNER JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000, query_plan_join_swap_table = 'false');

SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l LEFT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l LEFT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000, query_plan_join_swap_table = 'false');

-- RIGHT / FULL exercise the non-joined-rows stream, which reads stored blocks directly.
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l RIGHT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l RIGHT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000, query_plan_join_swap_table = 'false');

SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l FULL JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l FULL JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000, query_plan_join_swap_table = 'false');

-- ANY strictness exercises the non-lazy AddedColumns path (keys are unique so the match is deterministic).
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l ANY LEFT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 0, max_bytes_in_join = 0, query_plan_join_swap_table = 'false')
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l ANY LEFT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'hash', enable_join_in_memory_compression = 1, max_bytes_in_join = 24000000, query_plan_join_swap_table = 'false');

-- parallel_hash and grace_hash build on the same HashJoin internals; enabling the setting must not
-- change results. (Their per-slot / per-bucket accounting differs, so just check no regression.)
SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l INNER JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l INNER JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'parallel_hash', enable_join_in_memory_compression = 1);

SELECT (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l LEFT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'grace_hash', enable_join_in_memory_compression = 0)
     = (SELECT sum(cityHash64(l.k, l.lv, r.rv, r.pad)) FROM jimc_left AS l LEFT JOIN jimc_right AS r ON l.k = r.k
            SETTINGS join_algorithm = 'grace_hash', enable_join_in_memory_compression = 1);

DROP TABLE jimc_left;
DROP TABLE jimc_right;
