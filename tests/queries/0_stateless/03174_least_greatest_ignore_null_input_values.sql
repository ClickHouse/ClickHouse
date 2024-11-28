-- Tests functions "greatest" and "least" with NULL arguments

SELECT 'Test with one const argument';
SELECT greatest(NULL), least(NULL);

SELECT 'Test with two const arguments';
SELECT greatest(1, NULL), least(1, NULL);
SELECT greatest(NULL, 1), least(NULL, 1);
SELECT greatest(NULL, 1.0), least(NULL, 1.0);
SELECT greatest(1.0, NULL), least(1.0, NULL);
SELECT greatest(NULL, 'a'), least(NULL, 'a');
SELECT greatest('a', NULL), least('a', NULL);

SELECT 'Test with one non-const argument';
SELECT greatest(materialize(NULL)), least(materialize(NULL));

SELECT 'Test with two non-const arguments';
SELECT greatest(materialize(1), NULL), least(materialize(1), NULL);
SELECT greatest(materialize(NULL), 1), least(materialize(NULL), 1);
SELECT greatest(materialize(NULL), 1.0), least(materialize(NULL), 1.0);
SELECT greatest(materialize(1.0), NULL), least(materialize(1.0), NULL);
SELECT greatest(materialize(NULL), 'a'), least(materialize(NULL), 'a');
SELECT greatest(materialize('a'), NULL), least(materialize('a'), NULL);

SELECT 'Special cases';
SELECT greatest(toNullable(1), 2), least(toNullable(1), 2);
SELECT greatest(toLowCardinality(1), NULL), least(toLowCardinality(1), NULL);
