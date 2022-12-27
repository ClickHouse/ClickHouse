-- Test: crash the server
SELECT 'fooooo' INTERSECT SELECT 'fooooo';
SELECT 'fooooo' EXCEPT SELECT 'fooooo';

-- Test: intersect return incorrect result for const column
SELECT 1 FROM numbers(10) INTERSECT SELECT 1 FROM numbers(10);
SELECT toString(1) FROM numbers(10) INTERSECT SELECT toString(1) FROM numbers(10);
SELECT '1' FROM numbers(10) INTERSECT SELECT '1' FROM numbers(10);

-- Test: except return incorrect result for const column
SELECT 2 FROM numbers(10) EXCEPT SELECT 1 FROM numbers(5);
SELECT toString(2) FROM numbers(10) EXCEPT SELECT toString(1) FROM numbers(5);
SELECT '2' FROM numbers(10) EXCEPT SELECT '1' FROM numbers(5);