-- Index analysis normalizes the constant into the type the key expression reads. A `DateTime64(6)`
-- constant truncated to `DateTime64(3)` becomes the exact preimage of a real key point, and
-- `notEquals` then excluded that point - dropping a row whose stored value really differs from the
-- constant. Such an atom must stay relaxed: the key point has more than one preimage.

DROP TABLE IF EXISTS t_lossy_const_key;

CREATE TABLE t_lossy_const_key (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY toString(d);

INSERT INTO t_lossy_const_key VALUES (toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC'));
INSERT INTO t_lossy_const_key VALUES (toDateTime64('2023-02-01 13:00:00.000', 3, 'UTC'));

SELECT 'a finer-scale constant';
SELECT count() FROM t_lossy_const_key WHERE d != toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC');
SELECT countIf(d != toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')) FROM t_lossy_const_key;
SELECT count() FROM t_lossy_const_key WHERE d = toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC');

SELECT 'a representable constant still prunes';
SELECT count() FROM t_lossy_const_key WHERE d != toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC');
SELECT count() FROM t_lossy_const_key WHERE d = toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC');

-- A `String` constant is not a value in its own domain: the comparison converts the spelling into the
-- type of the column and compares there, so a spelling that renders back differently - `12:00:00`
-- comes back as `12:00:00.000` - still names exactly one key point and keeps its pruning.
SELECT 'a String spelling of the same point';
SELECT count() FROM t_lossy_const_key WHERE d != '2023-02-01 12:00:00';
SELECT countIf(d != '2023-02-01 12:00:00') FROM t_lossy_const_key;

-- Reporting a lossless normalization as lossy would silently disable pruning, which no answer can
-- show. `max_rows_to_read` pins it: with the part holding the excluded key point pruned, the query
-- reads the other row only.
SELECT 'the lossless normalizations prune';
SELECT count() FROM t_lossy_const_key WHERE d != toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC')
    SETTINGS max_rows_to_read = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT count() FROM t_lossy_const_key WHERE d != toDateTime64('2023-02-01 12:00:00', 0, 'UTC')
    SETTINGS max_rows_to_read = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT count() FROM t_lossy_const_key WHERE d != '2023-02-01 12:00:00.000'
    SETTINGS max_rows_to_read = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT count() FROM t_lossy_const_key WHERE d != '2023-02-01 12:00:00'
    SETTINGS max_rows_to_read = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;

SELECT 'the lossy normalization does not prune';
SELECT count() FROM t_lossy_const_key WHERE d != toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')
    SETTINGS max_rows_to_read = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0; -- { serverError TOO_MANY_ROWS }

-- `notIn` and `notHas` exclude a key point just like `notEquals`, so a truncated set element must not
-- exclude it either. A literal set is coerced to the type of the left argument before the index sees
-- it, which makes the index and the rows agree by construction; a subquery set keeps its own type.
SELECT 'a finer-scale set element';
SELECT count() FROM t_lossy_const_key WHERE d NOT IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));
SELECT countIf(d NOT IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'))) FROM t_lossy_const_key;
SELECT count() FROM t_lossy_const_key WHERE d NOT IN (toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));
SELECT countIf(d NOT IN (toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'))) FROM t_lossy_const_key;
SELECT count() FROM t_lossy_const_key WHERE NOT has([toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')], d);
SELECT countIf(NOT has([toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')], d)) FROM t_lossy_const_key;
SELECT count() FROM t_lossy_const_key WHERE d IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));
SELECT countIf(d IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'))) FROM t_lossy_const_key;

DROP TABLE t_lossy_const_key;

-- The set path normalizes an element into the key type with no key transform involved at all, so a
-- bare key has the same hole, in both directions: `notIn` excluded a point the predicate does not
-- name, and `in` was read as certainly true over that point, which answered a `count()` from the
-- part without filtering its rows at all.
SELECT 'the same set element against a bare key';

DROP TABLE IF EXISTS t_lossy_const_plain;

CREATE TABLE t_lossy_const_plain (d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY d;

INSERT INTO t_lossy_const_plain VALUES (toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC'));
INSERT INTO t_lossy_const_plain VALUES (toDateTime64('2023-02-01 13:00:00.000', 3, 'UTC'));

SELECT count() FROM t_lossy_const_plain WHERE d NOT IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));
SELECT countIf(d NOT IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'))) FROM t_lossy_const_plain;
SELECT count() FROM t_lossy_const_plain WHERE d NOT IN (toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));
SELECT countIf(d NOT IN (toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'))) FROM t_lossy_const_plain;
SELECT count() FROM t_lossy_const_plain WHERE NOT has([toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')], d);
SELECT countIf(NOT has([toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC')], d)) FROM t_lossy_const_plain;
SELECT count() FROM t_lossy_const_plain WHERE d IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'));
SELECT countIf(d IN (SELECT toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC'))) FROM t_lossy_const_plain;

-- A lossless element keeps its pruning here too: one row of the subquery plus the row of the part the
-- set index did not exclude.
SELECT 'a lossless set element prunes';
SELECT count() FROM t_lossy_const_plain WHERE d NOT IN (SELECT toDateTime64('2023-02-01 12:00:00', 0, 'UTC'))
    SETTINGS max_rows_to_read = 2, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;

DROP TABLE t_lossy_const_plain;

SELECT 'the same shape in a partition key';

DROP TABLE IF EXISTS t_lossy_const_part;

CREATE TABLE t_lossy_const_part (d DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY toString(d) ORDER BY tuple();

INSERT INTO t_lossy_const_part VALUES (toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC')), (toDateTime64('2023-02-01 13:00:00.000', 3, 'UTC'));

SELECT count() FROM t_lossy_const_part WHERE d != toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC');
SELECT count() FROM t_lossy_const_part WHERE d != toDateTime64('2023-02-01 12:00:00.001', 3, 'UTC');
SELECT count() FROM t_lossy_const_part WHERE d = toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC');

DROP TABLE t_lossy_const_part;

SELECT 'a String constant that is not the key rendering';

DROP TABLE IF EXISTS t_lossy_const_str;

CREATE TABLE t_lossy_const_str (s String) ENGINE = MergeTree ORDER BY toInt64(s);

INSERT INTO t_lossy_const_str VALUES ('7');

SELECT count() FROM t_lossy_const_str WHERE s != '007';
SELECT countIf(s != '007') FROM t_lossy_const_str;

DROP TABLE t_lossy_const_str;
