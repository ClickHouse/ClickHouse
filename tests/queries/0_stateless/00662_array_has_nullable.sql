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
