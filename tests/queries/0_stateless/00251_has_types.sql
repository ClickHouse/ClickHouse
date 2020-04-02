SELECT has([1, 2, 3], 3.0);
SELECT has([1, 2.0, 3], 2);
SELECT has([1, 2.1, 3], 2);
SELECT has([1, -1], 1);
SELECT has([1, -1], 1000);

SELECT has(materialize([1, 2, 3]), 3.0);
SELECT has(materialize([1, 2.0, 3]), 2);
SELECT has(materialize([1, 2.1, 3]), 2);
SELECT has(materialize([1, -1]), 1);
SELECT has(materialize([1, -1]), 1000);

SELECT has([1, 2, 3], materialize(3.0));
SELECT has([1, 2.0, 3], materialize(2));
SELECT has([1, 2.1, 3], materialize(2));
SELECT has([1, -1], materialize(1));
SELECT has([1, -1], materialize(1000));

SELECT has(materialize([1, 2, 3]), materialize(3.0));
SELECT has(materialize([1, 2.0, 3]), materialize(2));
SELECT has(materialize([1, 2.1, 3]), materialize(2));
SELECT has(materialize([1, -1]), materialize(1));
SELECT has(materialize([1, -1]), materialize(1000));
