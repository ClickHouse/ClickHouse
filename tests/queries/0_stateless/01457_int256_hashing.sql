SELECT toUInt256(123) IN (NULL);
SELECT toUInt256(123) AS k GROUP BY k;
SELECT toUInt256(123) AS k FROM system.one INNER JOIN (SELECT toUInt256(123) AS k) t USING k;
SELECT arrayEnumerateUniq([toUInt256(123), toUInt256(456), toUInt256(123)]);

SELECT toInt256(123) IN (NULL);
SELECT toInt256(123) AS k GROUP BY k;
SELECT toInt256(123) AS k FROM system.one INNER JOIN (SELECT toInt256(123) AS k) t USING k;
SELECT arrayEnumerateUniq([toInt256(123), toInt256(456), toInt256(123)]);

-- SELECT toUInt128(123) IN (NULL);
-- SELECT toUInt128(123) AS k GROUP BY k;
-- SELECT toUInt128(123) AS k FROM system.one INNER JOIN (SELECT toUInt128(123) AS k) t USING k;
-- SELECT arrayEnumerateUniq([toUInt128(123), toUInt128(456), toUInt128(123)]);

SELECT toInt128(123) IN (NULL);
SELECT toInt128(123) AS k GROUP BY k;
SELECT toInt128(123) AS k FROM system.one INNER JOIN (SELECT toInt128(123) AS k) t USING k;
SELECT arrayEnumerateUniq([toInt128(123), toInt128(456), toInt128(123)]);
