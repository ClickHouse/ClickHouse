SET allow_introspection_functions = 1;

SELECT length(flameGraph([toUInt64(1)], -9223372036854775807 - 1, toUInt64(1))) >= 0
FROM system.one;

SELECT length(flameGraphMerge(s)) >= 0
FROM
(
    SELECT flameGraphState([toUInt64(1)], -9223372036854775807 - 1, toUInt64(1)) AS s FROM system.one
    UNION ALL
    SELECT flameGraphState([toUInt64(1)], -9223372036854775807 - 1, toUInt64(1)) AS s FROM system.one
);
