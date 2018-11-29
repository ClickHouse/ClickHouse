SET output_format_write_statistics = 0;
SELECT goals_alias.ID AS `ym:s:goalDimension`, uniqIf(UserID, (UserID != 0) AND (`_uniq_Goals` = 1))  FROM test.visits ARRAY JOIN Goals AS goals_alias,  arrayEnumerateUniq(Goals.ID)   AS `_uniq_Goals`  WHERE (CounterID = 101024) GROUP BY `ym:s:goalDimension` WITH TOTALS ORDER BY `ym:s:goalDimension` LIMIT 0, 1 FORMAT JSONCompact;
