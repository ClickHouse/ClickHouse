-- const tuple argument

SELECT tupleElement(('hello', 'world'), 1, 'default');
SELECT tupleElement(('hello', 'world'), 2, 'default');
SELECT tupleElement(('hello', 'world'), 3, 'default');
SELECT tupleElement(('hello', 'world'), 'xyz', 'default');
SELECT tupleElement(('hello', 'world'), 3, [([('a')], 1)]); -- arbitrary default value

SELECT tupleElement([(1, 2), (3, 4)], 1, 'default');
SELECT tupleElement([(1, 2), (3, 4)], 2, 'default');
SELECT tupleElement([(1, 2), (3, 4)], 3, 'default');

SELECT '--------';

-- non-const tuple argument

SELECT tupleElement(materialize(('hello', 'world')), 1, 'default');
SELECT tupleElement(materialize(('hello', 'world')), 2, 'default');
SELECT tupleElement(materialize(('hello', 'world')), 3, 'default');
SELECT tupleElement(materialize(('hello', 'world')), 'xzy', 'default');
SELECT tupleElement(materialize(('hello', 'world')), 'xzy', [([('a')], 1)]); -- arbitrary default value

SELECT tupleElement([[(count('2147483646'), 1)]], 'aaaa', [[1, 2, 3]]) -- bug #51525
