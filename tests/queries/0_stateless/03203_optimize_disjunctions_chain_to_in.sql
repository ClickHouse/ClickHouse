SET allow_experimental_analyzer=1;
CREATE TABLE foo (i Date) ENGINE MergeTree ORDER BY i;
INSERT INTO foo VALUES ('2020-01-01');
INSERT INTO foo VALUES ('2020-01-02');

SET optimize_min_equality_disjunction_chain_length = 3;
SELECT *
FROM foo
WHERE (foo.i = parseDateTimeBestEffort('2020-01-01'))
   OR (foo.i = parseDateTimeBestEffort('2020-01-02'))
   OR (foo.i = parseDateTimeBestEffort('2020-01-03'))
ORDER BY foo.i ASC
