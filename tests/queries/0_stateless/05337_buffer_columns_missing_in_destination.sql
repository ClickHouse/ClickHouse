-- A Buffer may have columns its destination lacks. A row must read the same while it is in the
-- Buffer and after it is flushed into the destination, where those columns are filled on read.

SET send_logs_level = 'error';

DROP TABLE IF EXISTS buf_s;
DROP TABLE IF EXISTS dst_s;
DROP TABLE IF EXISTS buf_m;
DROP TABLE IF EXISTS dst_m;
DROP TABLE IF EXISTS buf_w;
DROP TABLE IF EXISTS dst_w;
DROP TABLE IF EXISTS buf_l;
DROP TABLE IF EXISTS dst_l;

CREATE TABLE dst_s (k UInt32, arr Array(UInt8), `n.a` Array(UInt8)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE buf_s
(
    k UInt32,
    arr Array(UInt8),
    `n.a` Array(UInt8),
    x UInt64 DEFAULT length(arr),
    y UInt64 DEFAULT x + 10,
    m UInt64 MATERIALIZED length(arr),
    z UInt64 DEFAULT arr.size0,
    c UInt8 DEFAULT 0,
    a2 Array(UInt8) DEFAULT [7, 8],
    t Tuple(p UInt8, q UInt8) DEFAULT (5, 6),
    nl Nullable(UInt8),
    `n.b` Array(UInt8),
    w UInt64 DEFAULT length(n.b)
)
ENGINE = Buffer(currentDatabase(), dst_s, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO buf_s (k, arr, `n.a`) VALUES (1, [1, 2, 3], [4, 5]), (2, [2], [6]);

CREATE TABLE dst_m (k UInt32, arr String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE buf_m (k UInt32, arr Array(UInt8), x UInt64 DEFAULT length(arr), z UInt64 DEFAULT arr.size0)
ENGINE = Buffer(currentDatabase(), dst_m, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO buf_m (k, arr) VALUES (1, [1, 2, 3]);

CREATE TABLE dst_w (k UInt32, s LowCardinality(String), j JSON) ENGINE = MergeTree ORDER BY k;
CREATE TABLE buf_w (k UInt32, s String, j JSON, l UInt64 DEFAULT length(s), x UInt64 DEFAULT j.a.b::UInt64, j2 JSON DEFAULT '{"c": 7}')
ENGINE = Buffer(currentDatabase(), dst_w, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO buf_w (k, s, j) VALUES (1, 'abcd', '{"a": {"b": 42}}');

SELECT 'before flush';

-- { echoOn }
SELECT count() FROM buf_s;
SELECT k, x, y FROM buf_s ORDER BY k;
SELECT x FROM buf_s ORDER BY x;
SELECT k, m FROM buf_s ORDER BY k;
SELECT k, z FROM buf_s ORDER BY k;
SELECT k, c FROM buf_s ORDER BY k;
SELECT k, w FROM buf_s ORDER BY k;
SELECT k, length(a2), t.p, nl.null FROM buf_s ORDER BY k SETTINGS optimize_functions_to_subcolumns = 1;
SELECT k, length(a2), t.p, nl.null FROM buf_s ORDER BY k SETTINGS optimize_functions_to_subcolumns = 0;
SELECT k, n.b FROM buf_s ORDER BY k;
SELECT k, x FROM buf_s PREWHERE has(arr, 2) ORDER BY k;
SELECT k, n.b FROM buf_s PREWHERE has(n.a, 4) ORDER BY k;
SELECT k FROM buf_s WHERE x = 3 ORDER BY k;
CREATE ROW POLICY p_05337 ON buf_s USING has(arr, 3) TO ALL;
SELECT k, x FROM buf_s ORDER BY k;
DROP ROW POLICY p_05337 ON buf_s;
SELECT k, arr, x, z FROM buf_m ORDER BY k;
SELECT k, x FROM buf_m ORDER BY k;
SELECT x FROM buf_m ORDER BY x;
SELECT count() FROM buf_m;
SELECT k, l, x, j2.c FROM buf_w ORDER BY k;
-- { echoOff }

OPTIMIZE TABLE buf_s;
OPTIMIZE TABLE buf_m;
OPTIMIZE TABLE buf_w;
SELECT 'after flush', (SELECT count() FROM dst_s), (SELECT count() FROM dst_m), (SELECT count() FROM dst_w);

-- { echoOn }
SELECT count() FROM buf_s;
SELECT k, x, y FROM buf_s ORDER BY k;
SELECT x FROM buf_s ORDER BY x;
SELECT k, m FROM buf_s ORDER BY k;
SELECT k, z FROM buf_s ORDER BY k;
SELECT k, c FROM buf_s ORDER BY k;
SELECT k, w FROM buf_s ORDER BY k;
SELECT k, length(a2), t.p, nl.null FROM buf_s ORDER BY k SETTINGS optimize_functions_to_subcolumns = 1;
SELECT k, length(a2), t.p, nl.null FROM buf_s ORDER BY k SETTINGS optimize_functions_to_subcolumns = 0;
SELECT k, n.b FROM buf_s ORDER BY k;
SELECT k, x FROM buf_s PREWHERE has(arr, 2) ORDER BY k;
SELECT k, n.b FROM buf_s PREWHERE has(n.a, 4) ORDER BY k;
SELECT k FROM buf_s WHERE x = 3 ORDER BY k;
CREATE ROW POLICY p_05337 ON buf_s USING has(arr, 3) TO ALL;
SELECT k, x FROM buf_s ORDER BY k;
DROP ROW POLICY p_05337 ON buf_s;
SELECT k, arr, x, z FROM buf_m ORDER BY k;
SELECT k, x FROM buf_m ORDER BY k;
SELECT x FROM buf_m ORDER BY x;
SELECT count() FROM buf_m;
SELECT k, l, x, j2.c FROM buf_w ORDER BY k;
-- { echoOff }

-- The lambda parameter `arr` is not a column the default reads. Without the plan optimizations,
-- which would prune it, reading and converting the column `arr` fails.
CREATE TABLE dst_l (k UInt32, arr String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dst_l VALUES (1, 'not an array');
CREATE TABLE buf_l (k UInt32, arr Array(UInt8), l UInt64 DEFAULT arraySum(arrayMap(arr -> arr + 1, [1, 2])))
ENGINE = Buffer(currentDatabase(), dst_l, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
SELECT k, l FROM buf_l SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE buf_s;
DROP TABLE dst_s;
DROP TABLE buf_m;
DROP TABLE dst_m;
DROP TABLE buf_w;
DROP TABLE dst_w;
DROP TABLE buf_l;
DROP TABLE dst_l;
