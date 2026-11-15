SELECT has(['a', 'b'], 'a');
SELECT has(['a', 'b'], 'b');
SELECT has(['a', 'b'], 'c');
SELECT has(['a', 'b'], NULL);

SELECT has(['a', NULL, 'b'], 'a');
SELECT has(['a', NULL, 'b'], 'b');
SELECT has(['a', NULL, 'b'], 'c');
SELECT has(['a', NULL, 'b'], NULL);

SELECT has(materialize(['a', 'b']), 'a');
SELECT has(materialize(['a', 'b']), 'b');
SELECT has(materialize(['a', 'b']), 'c');
SELECT has(materialize(['a', 'b']), NULL);

SELECT has(materialize(['a', NULL, 'b']), 'a');
SELECT has(materialize(['a', NULL, 'b']), 'b');
SELECT has(materialize(['a', NULL, 'b']), 'c');
SELECT has(materialize(['a', NULL, 'b']), NULL);

SELECT has(['a', 'b'], materialize('a'));
SELECT has(['a', 'b'], materialize('b'));
SELECT has(['a', 'b'], materialize('c'));

SELECT has(['a', NULL, 'b'], materialize('a'));
SELECT has(['a', NULL, 'b'], materialize('b'));
SELECT has(['a', NULL, 'b'], materialize('c'));

SELECT has(materialize(['a', 'b']), materialize('a'));
SELECT has(materialize(['a', 'b']), materialize('b'));
SELECT has(materialize(['a', 'b']), materialize('c'));

SELECT has(materialize(['a', NULL, 'b']), materialize('a'));
SELECT has(materialize(['a', NULL, 'b']), materialize('b'));
SELECT has(materialize(['a', NULL, 'b']), materialize('c'));


SELECT has(array_value, needle), indexOf(array_value, needle), countEqual(array_value, needle)
FROM VALUES('array_value Array(String), needle Nullable(String)',
    ([''], NULL),
    ([''], ''),
    (['a'], NULL),
    (['a'], 'a'),
    (['a', ''], NULL),
    (['a', ''], 'a'),
    ([], NULL));

SELECT has(array_value, needle), indexOf(array_value, needle), countEqual(array_value, needle)
FROM VALUES('array_value Array(Nullable(String)), needle Nullable(String)',
    ([NULL], NULL),
    ([''], NULL),
    (['a'], NULL),
    (['a', NULL], NULL),
    (['a', ''], ''),
    (['a', NULL], 'a'),
    ([], NULL));

SELECT has(array_value, needle), indexOf(array_value, needle), countEqual(array_value, needle)
FROM VALUES('array_value Array(Nullable(String)), needle Nullable(String)',
    (['', NULL, '', NULL], NULL));

SELECT indexOfAssumeSorted(array_value, needle)
FROM VALUES('array_value Array(String), needle Nullable(String)',
    (['', 'a'], NULL));

SELECT notHas(array_value, needle)
FROM VALUES('array_value Array(String), needle Nullable(String)',
    ([''], NULL));

SELECT mapContainsKey(m, needle), mapContainsValue(m, needle)
FROM VALUES('m Map(String, String), needle Nullable(String)',
    (map('', ''), NULL));


SELECT has([111, 222], 111);
SELECT has([111, 222], 222);
SELECT has([111, 222], 333);
SELECT has([111, 222], NULL);

SELECT has([111, NULL, 222], 111);
SELECT has([111, NULL, 222], 222);
SELECT has([111, NULL, 222], 333);
SELECT has([111, NULL, 222], NULL);

SELECT has(materialize([111, 222]), 111);
SELECT has(materialize([111, 222]), 222);
SELECT has(materialize([111, 222]), 333);
SELECT has(materialize([111, 222]), NULL);

SELECT has(materialize([111, NULL, 222]), 111);
SELECT has(materialize([111, NULL, 222]), 222);
SELECT has(materialize([111, NULL, 222]), 333);
SELECT has(materialize([111, NULL, 222]), NULL);

SELECT has([111, 222], materialize(111));
SELECT has([111, 222], materialize(222));
SELECT has([111, 222], materialize(333));

SELECT has([111, NULL, 222], materialize(111));
SELECT has([111, NULL, 222], materialize(222));
SELECT has([111, NULL, 222], materialize(333));

SELECT has(materialize([111, 222]), materialize(111));
SELECT has(materialize([111, 222]), materialize(222));
SELECT has(materialize([111, 222]), materialize(333));

SELECT has(materialize([111, NULL, 222]), materialize(111));
SELECT has(materialize([111, NULL, 222]), materialize(222));
SELECT has(materialize([111, NULL, 222]), materialize(333));
