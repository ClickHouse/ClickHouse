SELECT countMerge(*) FROM (SELECT countState(0.5) AS a UNION ALL SELECT countState() UNION ALL SELECT countIfState(2, 1) UNION ALL SELECT countArrayState([1, 2]) UNION ALL SELECT countArrayIfState([1, 2], 1));

SELECT quantileMerge(*) FROM (SELECT quantilesState(0.5)(1) AS a UNION ALL SELECT quantileStateIf(2, identity(1)));
