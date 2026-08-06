-- Regression test: `IN` with a non-constant right-hand side that does not reference any column,
-- e.g. `materialize(1)`. Building a constant `Set` from it fails with "not a constant expression",
-- so it has to take the same row-wise rewrite as a right-hand side with identifiers.

-- { echoOn }

SET enable_analyzer = 0;

SELECT number IN (materialize(1)) FROM numbers(3);
SELECT number NOT IN (materialize(1)) FROM numbers(3);
SELECT number IN (materialize(1), 2) FROM numbers(3);
SELECT number IN materialize([0, 2]) FROM numbers(3);
SELECT (number, number + 1) IN (materialize((1, 2))) FROM numbers(3);
SELECT number IN (if(materialize(1) = 1, 1, 2)) FROM numbers(3);
SELECT 1 IN (materialize(1));
SELECT NULL IN (materialize(1));
SELECT materialize(NULL) IN (materialize(1));
SELECT number IN (materialize(NULL), 1) FROM numbers(2);

-- Constant enumerations must keep working via the constant `Set` path.
SELECT number IN (0, 2) FROM numbers(3);
SELECT number IN (concat('a', 'b') = 'ab', 2) FROM numbers(3);

SET enable_analyzer = 1;

SELECT number IN (materialize(1)) FROM numbers(3);
SELECT number NOT IN (materialize(1)) FROM numbers(3);
SELECT number IN (materialize(1), 2) FROM numbers(3);
SELECT number IN materialize([0, 2]) FROM numbers(3);
SELECT (number, number + 1) IN (materialize((1, 2))) FROM numbers(3);
SELECT number IN (if(materialize(1) = 1, 1, 2)) FROM numbers(3);
SELECT 1 IN (materialize(1));
SELECT NULL IN (materialize(1));
SELECT materialize(NULL) IN (materialize(1));
SELECT number IN (materialize(NULL), 1) FROM numbers(2);

SELECT number IN (0, 2) FROM numbers(3);
SELECT number IN (concat('a', 'b') = 'ab', 2) FROM numbers(3);
