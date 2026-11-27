-- `has` compares named tuples positionally, while a `CAST` between named tuples matches their fields
-- by name, so a set element whose fields are declared in another order transforms to a key value that
-- no matching row holds. That must not prune the granule or the part that holds it - not even when the
-- key is a non-injective function of the column and the atom is relaxed.

DROP TABLE IF EXISTS t_has_named_tuple;

CREATE TABLE t_has_named_tuple (t Tuple(b UInt8, a UInt8)) ENGINE = MergeTree ORDER BY sipHash64(t)
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_named_tuple VALUES ((1, 2)), ((3, 4)), ((5, 6));

SELECT sum(has([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], t)) FROM t_has_named_tuple;
SELECT count() FROM t_has_named_tuple WHERE has([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], t);
SELECT count() FROM t_has_named_tuple WHERE has([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], t) SETTINGS use_primary_key = 0;

-- An element with the key's own field order still prunes.
SELECT count() FROM t_has_named_tuple WHERE has([CAST((1, 2), 'Tuple(b UInt8, a UInt8)')], t);
SELECT count() FROM t_has_named_tuple WHERE has([CAST((7, 8), 'Tuple(b UInt8, a UInt8)')], t);

DROP TABLE t_has_named_tuple;

-- The same through a partition key.

CREATE TABLE t_has_named_tuple (t Tuple(b UInt8, a UInt8)) ENGINE = MergeTree PARTITION BY sipHash64(t) ORDER BY tuple()
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_named_tuple VALUES ((1, 2)), ((3, 4)), ((5, 6));

SELECT count() FROM t_has_named_tuple WHERE has([CAST((1, 2), 'Tuple(a UInt8, b UInt8)')], t);
SELECT count() FROM t_has_named_tuple WHERE has([CAST((1, 2), 'Tuple(b UInt8, a UInt8)')], t);

DROP TABLE t_has_named_tuple;

-- A pair whose raw comparison raises must not be answered by pruning: relaxing an atom permits extra
-- rows, not turning an exception into an empty result.

DROP TABLE IF EXISTS t_has_ip_key;

CREATE TABLE t_has_ip_key (x UInt32) ENGINE = MergeTree PARTITION BY intDiv(x, 1000000) ORDER BY x
SETTINGS add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_has_ip_key VALUES (1), (2000000);

SELECT count() FROM t_has_ip_key WHERE has([toIPv4('1.2.3.4')], x); -- { serverError BAD_TYPE_OF_FIELD }

DROP TABLE t_has_ip_key;
