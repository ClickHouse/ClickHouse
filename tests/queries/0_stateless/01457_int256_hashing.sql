-- Tags: no-fasttest

SET joined_subquery_requires_alias = 0;

SELECT toUInt256(123) IN (NULL);
SELECT toUInt256(123) AS k GROUP BY k;
SELECT k FROM (SELECT toUInt256(123) AS k FROM system.one) INNER JOIN (SELECT toUInt256(123) AS k) t USING k;
SELECT arrayEnumerateUniq([toUInt256(123), toUInt256(456), toUInt256(123)]);

SELECT toInt256(123) IN (NULL);
SELECT toInt256(123) AS k GROUP BY k;
SELECT k FROM (SELECT toInt256(123) AS k FROM system.one) INNER JOIN (SELECT toInt256(123) AS k) t USING k;
SELECT arrayEnumerateUniq([toInt256(123), toInt256(456), toInt256(123)]);

-- SELECT toUInt128(123) IN (NULL);
-- SELECT toUInt128(123) AS k GROUP BY k;
-- SELECT toUInt128(123) AS k FROM system.one INNER JOIN (SELECT toUInt128(123) AS k) t USING k;
-- SELECT arrayEnumerateUniq([toUInt128(123), toUInt128(456), toUInt128(123)]);

SELECT toInt128(123) IN (NULL);
SELECT toInt128(123) AS k GROUP BY k;
SELECT k FROM (SELECT toInt128(123) AS k FROM system.one) INNER JOIN (SELECT toInt128(123) AS k) t USING k;
SELECT arrayEnumerateUniq([toInt128(123), toInt128(456), toInt128(123)]);

SELECT toNullable(toUInt256(321)) IN (NULL);
SELECT toNullable(toUInt256(321)) AS k GROUP BY k;
SELECT k FROM (SELECT toNullable(toUInt256(321)) AS k FROM system.one) INNER JOIN (SELECT toUInt256(321) AS k) t USING k;
SELECT arrayEnumerateUniq([toNullable(toUInt256(321)), toNullable(toUInt256(456)), toNullable(toUInt256(321))]);

SELECT toNullable(toInt256(321)) IN (NULL);
SELECT toNullable(toInt256(321)) AS k GROUP BY k;
SELECT k FROM (SELECT toNullable(toInt256(321)) AS k FROM system.one) INNER JOIN (SELECT toInt256(321) AS k) t USING k;
SELECT arrayEnumerateUniq([toNullable(toInt256(321)), toNullable(toInt256(456)), toNullable(toInt256(321))]);

-- SELECT toNullable(toUInt128(321)) IN (NULL);
-- SELECT toNullable(toUInt128(321)) AS k GROUP BY k;
-- SELECT toNullable(toUInt128(321)) AS k FROM system.one INNER JOIN (SELECT toUInt128(321) AS k) t USING k;
-- SELECT arrayEnumerateUniq([toNullable(toUInt128(321)), toNullable(toUInt128(456)), toNullable(toUInt128(321))]);

SELECT toNullable(toInt128(321)) IN (NULL);
SELECT toNullable(toInt128(321)) AS k GROUP BY k;
SELECT k FROM (SELECT toNullable(toInt128(321)) AS k FROM system.one) INNER JOIN (SELECT toInt128(321) AS k) t USING k;
SELECT arrayEnumerateUniq([toNullable(toInt128(321)), toNullable(toInt128(456)), toNullable(toInt128(321))]);
