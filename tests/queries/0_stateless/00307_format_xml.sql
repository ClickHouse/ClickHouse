SET output_format_write_statistics = 0;
SELECT 'Hello & world' AS s, 'Hello\n<World>', toDateTime('2001-02-03 04:05:06') AS time, arrayMap(x -> toString(x), range(10)) AS arr, (s, time) AS tpl SETTINGS extremes = 1 FORMAT XML;
