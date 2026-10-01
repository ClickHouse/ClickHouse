-- An ALIAS column whose expression depends only on GROUP BY keys can be used after aggregation.
-- https://github.com/ClickHouse/ClickHouse/issues/121400

DROP TABLE IF EXISTS tx_local;

CREATE TABLE tx_local
(
    transaction_id String,
    sender_address String,
    token_outgoing_value Float64,
    sender_cluster_id UInt64 ALIAS farmFingerprint64(sender_address)
) ENGINE = MergeTree ORDER BY transaction_id;

INSERT INTO tx_local VALUES ('t1', 'a', 1.0), ('t1', 'b', 2.0), ('t2', 'a', 3.0);

SELECT transaction_id, sender_address AS address, toString(sender_cluster_id) AS cluster_id, sum(token_outgoing_value) AS value
FROM tx_local
GROUP BY transaction_id, address
ORDER BY transaction_id, address;

DROP TABLE tx_local;

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    k String,
    n UInt64,
    v UInt64,
    upper_k String ALIAS upper(k),
    upper_k_excl String ALIAS concat(upper_k, '!'),
    upper_k_same String ALIAS upper_k,
    len_k UInt8 ALIAS length(k) + 1,
    n_mod UInt64 ALIAS n % 3,
    const_alias UInt8 ALIAS 42
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t VALUES ('a', 1, 10), ('b', 2, 20), ('bb', 3, 30), ('a', 4, 40);

SELECT '-- expression of a key';
SELECT k, upper_k, sum(v) FROM t GROUP BY k ORDER BY k;
SELECT upper_k, sum(v) FROM t GROUP BY upper(k) ORDER BY upper_k;

SELECT '-- ALIAS column referencing other ALIAS columns';
SELECT k, upper_k_excl, upper_k_same, sum(v) FROM t GROUP BY k ORDER BY k;
SELECT upper_k_excl, sum(v) FROM t GROUP BY upper_k ORDER BY upper_k_excl;

SELECT '-- declared type differs from the type of the expression';
SELECT k, len_k, toTypeName(len_k), sum(v) FROM t GROUP BY k ORDER BY k;

SELECT '-- constant ALIAS expression';
SELECT const_alias, count() FROM t;

SELECT '-- HAVING, ORDER BY, LIMIT BY';
SELECT k, sum(v) AS s FROM t GROUP BY k HAVING upper_k != 'B' AND s > 0 ORDER BY upper_k DESC;
SELECT k, sum(v) FROM t GROUP BY k ORDER BY k LIMIT 1 BY len_k;

SELECT '-- window function over the aggregated result';
SELECT k, sum(sum(v)) OVER (PARTITION BY len_k) AS s FROM t GROUP BY k ORDER BY k;

SELECT '-- ALIAS column is a key or is under an aggregate function';
SELECT upper_k, count() FROM t GROUP BY upper_k ORDER BY upper_k;
SELECT k, max(upper_k), sum(n_mod) FROM t GROUP BY k ORDER BY k;

SELECT '-- ROLLUP and GROUPING SETS';
SELECT k, upper_k, sum(v) FROM t GROUP BY k WITH ROLLUP ORDER BY k;
SELECT k, upper_k, n, sum(v) FROM t GROUP BY GROUPING SETS ((k), (n)) ORDER BY k, n;

SELECT '-- remote';
SELECT k, upper_k, sum(v) FROM remote('127.0.0.{1,2}', currentDatabase(), t) GROUP BY k ORDER BY k;

SELECT '-- errors';
SELECT k, n_mod FROM t GROUP BY k; -- { serverError NOT_AN_AGGREGATE }
SELECT upper_k, count() FROM t; -- { serverError NOT_AN_AGGREGATE }
SELECT k, upper_k, sum(v) FROM t GROUP BY k WITH ROLLUP SETTINGS group_by_use_nulls = 1; -- { serverError NOT_AN_AGGREGATE }

DROP TABLE t;
