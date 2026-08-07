SELECT a FROM (SELECT ignore((SELECT 1)) AS a, a AS b);

SELECT x FROM (SELECT dummy AS x, plus(ignore(ignore(ignore(ignore('-922337203.6854775808', ignore(NULL)), ArrLen = 256, ignore(100, Arr.C3, ignore(NULL), (SELECT 10.000100135803223, count(*) FROM system.time_zones) > NULL)))), dummy, 65535) AS dummy ORDER BY ignore(-2) ASC, identity(x) DESC NULLS FIRST) FORMAT Null; -- { serverError UNKNOWN_IDENTIFIER }
