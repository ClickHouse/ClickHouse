CREATE TEMPORARY TABLE enum (x Enum('hello' = 1, 'world' = 2));
INSERT INTO enum VALUES ('hello');

SELECT count() FROM enum WHERE x = 'hello';
SELECT count() FROM enum WHERE x = 'world';
SELECT count() FROM enum WHERE x = 'xyz'; -- { serverError 36 }
