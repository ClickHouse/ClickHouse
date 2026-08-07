SELECT [(1, 'Hello'), (2, 'World')] AS nested, nested.1, nested.2;
SELECT [[(1, 'Hello'), (2, 'World')], [(3, 'Goodbye')]] AS nested, nested.1, nested.2;
