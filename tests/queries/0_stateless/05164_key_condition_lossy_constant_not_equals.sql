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

DROP TABLE t_lossy_const_key;

SELECT 'the same shape in a partition key';

CREATE TABLE t_lossy_const_part (d DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY toString(d) ORDER BY tuple();

INSERT INTO t_lossy_const_part VALUES (toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC')), (toDateTime64('2023-02-01 13:00:00.000', 3, 'UTC'));

SELECT count() FROM t_lossy_const_part WHERE d != toDateTime64('2023-02-01 12:00:00.000001', 6, 'UTC');
SELECT count() FROM t_lossy_const_part WHERE d != toDateTime64('2023-02-01 12:00:00.001', 3, 'UTC');
SELECT count() FROM t_lossy_const_part WHERE d = toDateTime64('2023-02-01 12:00:00.000', 3, 'UTC');

DROP TABLE t_lossy_const_part;

SELECT 'a String constant that is not the key rendering';

CREATE TABLE t_lossy_const_str (s String) ENGINE = MergeTree ORDER BY toInt64(s);

INSERT INTO t_lossy_const_str VALUES ('7');

SELECT count() FROM t_lossy_const_str WHERE s != '007';
SELECT countIf(s != '007') FROM t_lossy_const_str;

DROP TABLE t_lossy_const_str;
