-- `joinGet` and `joinGetOrNull` probe the Join table's partitioned hash join one block at a time;
-- their results are checked against the same lookups written as joins over a Memory table.

SET join_algorithm = 'hash';

DROP TABLE IF EXISTS jg_ref;
DROP TABLE IF EXISTS jg_any;
DROP TABLE IF EXISTS jg_any_last;
DROP TABLE IF EXISTS jg_two_keys;
DROP TABLE IF EXISTS jg_str;
DROP TABLE IF EXISTS jg_nullable;

-- 60000 rows over 20000 keys: the table doubles several times while the rows arrive.
CREATE TABLE jg_ref (k UInt32, s String, v Int64) ENGINE = Memory;
INSERT INTO jg_ref SELECT number % 20000, concat('s', toString(number)), toInt64(number) FROM numbers(60000);

CREATE TABLE jg_any (k UInt32, s String, v Int64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE jg_any_last (k UInt32, s String, v Int64) ENGINE = Join(ANY, LEFT, k) SETTINGS join_any_take_last_row = 1;
INSERT INTO jg_any SELECT * FROM jg_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 4096;
INSERT INTO jg_any_last SELECT * FROM jg_ref ORDER BY v SETTINGS max_threads = 1, max_block_size = 4096;

SELECT 'keys', count() FROM jg_any;

-- Half of the probed keys are missing: `joinGet` returns the default, `joinGetOrNull` NULL.
SELECT 'joinGet first',
    (SELECT (count(), sum(cityHash64(k, v, s))) FROM (SELECT number AS k, joinGet('jg_any', 'v', toUInt32(number)) AS v, joinGet('jg_any', 's', toUInt32(number)) AS s FROM numbers(40000)))
    = (SELECT (count(), sum(cityHash64(k, v, s))) FROM (SELECT l.k, r.v_agg AS v, r.s_agg AS s FROM (SELECT toUInt32(number) AS k FROM numbers(40000)) AS l ANY LEFT JOIN (SELECT k, min(v) AS v_agg, argMin(s, v) AS s_agg FROM jg_ref GROUP BY k) AS r USING (k)));

SELECT 'joinGet last',
    (SELECT (count(), sum(cityHash64(k, v))) FROM (SELECT number AS k, joinGet('jg_any_last', 'v', toUInt32(number)) AS v FROM numbers(40000)))
    = (SELECT (count(), sum(cityHash64(k, v))) FROM (SELECT l.k, r.v_agg AS v FROM (SELECT toUInt32(number) AS k FROM numbers(40000)) AS l ANY LEFT JOIN (SELECT k, max(v) AS v_agg FROM jg_ref GROUP BY k) AS r USING (k)));

SELECT 'joinGetOrNull',
    (SELECT (count(), sum(cityHash64(k, v))) FROM (SELECT number AS k, joinGetOrNull('jg_any', 'v', toUInt32(number)) AS v FROM numbers(40000)))
    = (SELECT (count(), sum(cityHash64(k, v))) FROM (SELECT l.k, r.v_agg AS v FROM (SELECT toUInt32(number) AS k FROM numbers(40000)) AS l ANY LEFT JOIN (SELECT k, min(v) AS v_agg FROM jg_ref GROUP BY k) AS r USING (k) SETTINGS join_use_nulls = 1));

SELECT 'missing', joinGet('jg_any', 'v', toUInt32(20000)), joinGet('jg_any', 's', toUInt32(20000)), joinGetOrNull('jg_any', 'v', toUInt32(20000)), joinGetOrNull('jg_any', 's', toUInt32(20000));
SELECT 'zero key', joinGet('jg_any', 'v', toUInt32(0)), joinGet('jg_any_last', 'v', toUInt32(0));
SELECT 'constant key', joinGet('jg_any', 'v', materialize(toUInt32(7))), joinGet('jg_any', 'v', toUInt32(7));

-- Composite, string and nullable keys.
CREATE TABLE jg_two_keys (a UInt64, b UInt64, v Int64) ENGINE = Join(ANY, LEFT, a, b);
INSERT INTO jg_two_keys SELECT number % 300, number % 7, toInt64(number) FROM numbers(2100);
SELECT 'two keys',
    (SELECT (count(), sum(cityHash64(a, b, v))) FROM (SELECT number % 400 AS a, number % 9 AS b, joinGet('jg_two_keys', 'v', toUInt64(number % 400), toUInt64(number % 9)) AS v FROM numbers(3600)))
    = (SELECT (count(), sum(cityHash64(a, b, v))) FROM (SELECT l.a, l.b, r.v_agg AS v FROM (SELECT number % 400 AS a, number % 9 AS b FROM numbers(3600)) AS l ANY LEFT JOIN (SELECT a, b, min(v) AS v_agg FROM (SELECT number % 300 AS a, number % 7 AS b, toInt64(number) AS v FROM numbers(2100)) GROUP BY a, b) AS r USING (a, b)));

CREATE TABLE jg_str (s String, v Int64) ENGINE = Join(ANY, LEFT, s);
INSERT INTO jg_str SELECT if(number % 100 = 0, '', concat('k', toString(number % 1000))), toInt64(number) FROM numbers(3000);
SELECT 'string key',
    (SELECT (count(), sum(cityHash64(s, v))) FROM (SELECT if(number % 50 = 0, '', concat('k', toString(number))) AS s, joinGet('jg_str', 'v', s) AS v FROM numbers(1500)))
    = (SELECT (count(), sum(cityHash64(s, v))) FROM (SELECT l.s, r.v_agg AS v FROM (SELECT if(number % 50 = 0, '', concat('k', toString(number))) AS s FROM numbers(1500)) AS l ANY LEFT JOIN (SELECT s, min(v) AS v_agg FROM (SELECT if(number % 100 = 0, '', concat('k', toString(number % 1000))) AS s, toInt64(number) AS v FROM numbers(3000)) GROUP BY s) AS r USING (s)));

CREATE TABLE jg_nullable (k Nullable(UInt32), v Int64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO jg_nullable SELECT if(number % 10 = 0, NULL, number % 100), toInt64(number) FROM numbers(1000);
SELECT 'nullable key', joinGet('jg_nullable', 'v', toNullable(toUInt32(5))), joinGet('jg_nullable', 'v', CAST(NULL, 'Nullable(UInt32)')), joinGetOrNull('jg_nullable', 'v', CAST(NULL, 'Nullable(UInt32)'));
SELECT 'nullable keys never join', count() FROM jg_nullable;

DROP TABLE jg_ref;
DROP TABLE jg_any;
DROP TABLE jg_any_last;
DROP TABLE jg_two_keys;
DROP TABLE jg_str;
DROP TABLE jg_nullable;
