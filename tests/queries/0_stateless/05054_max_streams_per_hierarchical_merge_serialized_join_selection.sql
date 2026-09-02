SELECT count()
FROM numbers(10) AS l
INNER JOIN numbers(10) AS r ON l.number = r.number
SETTINGS serialize_query_plan = 1, join_algorithm = 'hash', max_streams_per_hierarchical_merge = 1;

SELECT count()
FROM numbers(10) AS l
INNER JOIN numbers(10) AS r ON l.number = r.number
SETTINGS serialize_query_plan = 1, join_algorithm = 'hash,full_sorting_merge', max_streams_per_hierarchical_merge = 1;

SELECT count()
FROM numbers(10) AS l
INNER JOIN numbers(10) AS r ON l.number = r.number
SETTINGS serialize_query_plan = 1, join_algorithm = 'hash,ie_join', max_streams_per_hierarchical_merge = 1;
