SET allow_deprecated_error_prone_window_functions = 1;
SELECT k, d, i FROM (SELECT t.1 AS k, t.2 AS v, runningDifference(v) AS d, runningDifference(cityHash64(t.1)) AS i FROM (SELECT arrayJoin([(NULL, 65535), ('a', 7), ('a', 3), ('b', 11), ('b', 2), ('', -9223372036854775808)]) AS t)) WHERE i = 9223372036854775807;
