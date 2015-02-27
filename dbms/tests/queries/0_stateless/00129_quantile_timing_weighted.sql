SELECT medianTiming(t), medianTimingWeighted(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 1 AS w FROM system.numbers LIMIT 100);
SELECT medianTiming(t), medianTimingWeighted(t, w) FROM (SELECT number AS t, number = 77 ? 10 : 0 AS w FROM system.numbers LIMIT 100);
SELECT medianTiming(t), medianTimingWeighted(t, w) FROM (SELECT number AS t, number = 77 ? 0 : 0 AS w FROM system.numbers LIMIT 100);
