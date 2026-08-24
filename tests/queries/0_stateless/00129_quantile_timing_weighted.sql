SELECT medianTiming(t), medianTimingWeighted(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 1 AS w FROM system.numbers LIMIT 100);
SELECT quantileTiming(0.5)(t), quantileTimingWeighted(0.5)(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 0 AS w FROM system.numbers LIMIT 100);
SELECT medianTiming(t), medianTimingWeighted(t, w) FROM (SELECT number AS t, number = 77 ? 0 : 0 AS w FROM system.numbers LIMIT 100);
SELECT quantilesTiming(0.5, 0.9)(t), quantilesTimingWeighted(0.5, 0.9)(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 1 AS w FROM system.numbers LIMIT 100);
