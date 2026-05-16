-- CARDINALITY is a SQL-standard / PostgreSQL alias of length for arrays.
SELECT CARDINALITY([1, 2, 3, 4]);
SELECT cardinality([]);
SELECT cardinality(['a', 'b']);
SELECT CARDINALITY('hello');
