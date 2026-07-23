SET enable_analyzer=1;

SELECT DISTINCT
    firstNonDefault(__table3.dummy, __table4.dummy) AS dummy,
    __table4.`isNotNull(map(assumeNotNull(isNull(3)), NULL))` AS `isNotNull(map(assumeNotNull(isNull(3)), NULL))`,
    __table4.`isNotDistinctFrom(3, isZeroOrNull(isNotNull(3)))` AS `isNotDistinctFrom(3, isZeroOrNull(isNotNull(3)))`
FROM system.one AS __table3
ALL FULL OUTER JOIN
(
    SELECT DISTINCT
        __table5.dummy AS dummy,
        __table5.`isNotNull(map(assumeNotNull(isNull(3)), NULL))` AS `isNotNull(map(assumeNotNull(isNull(3)), NULL))`,
        _CAST(0, 'UInt8') AS `isNotDistinctFrom(3, isZeroOrNull(isNotNull(3)))`
    FROM
    (
        SELECT
            __table6.dummy AS dummy,
            _CAST(1, 'UInt8') AS `isNotNull(map(assumeNotNull(isNull(3)), NULL))`
        FROM remote('127.0.0.3') AS __table6
    ) AS __table5
) AS __table4 USING (dummy)
LEFT ARRAY JOIN [equals(materialize(3), _CAST(1, 'UInt8'))] AS __array_join_exp_2
LEFT ARRAY JOIN map(_CAST(0, 'UInt8'), materialize(2), _CAST(0, 'UInt8'), _CAST('1', 'UInt128')) AS __array_join_exp_1
GROUP BY
    materialize(1),
    __table4.`isNotNull(map(assumeNotNull(isNull(3)), NULL))`,
    firstNonDefault(__table3.dummy, __table4.dummy),
    __table4.`isNotDistinctFrom(3, isZeroOrNull(isNotNull(3)))`,
    __table4.`isNotNull(map(assumeNotNull(isNull(3)), NULL))`
    WITH CUBE
SETTINGS enable_lazy_columns_replication = 1;


