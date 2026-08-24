SELECT DISTINCT toFloat64(arrayJoin(['+inf', '+Inf', '+INF', '+infinity', '+Infinity']));
SELECT DISTINCT toFloat64(arrayJoin(['-inf', '-Inf', '-INF', '-infinity', '-Infinity']));
SELECT DISTINCT toFloat64(arrayJoin(['inf', 'Inf', 'INF', 'infinity', 'Infinity']));
