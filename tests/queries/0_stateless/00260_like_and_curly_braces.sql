SELECT 'a}a' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a}a' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT 'a}a' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a}a' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;

SELECT 'a{a' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a{a' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT 'a{a' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a{a' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;

SELECT '{a' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT '{a' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT '{a' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT '{a' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;

SELECT 'a{' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a{' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT 'a{' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a{' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;

SELECT 'a}' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a}' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT 'a}' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT 'a}' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;

SELECT '}a' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT '}a' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT '}a' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT '}a' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;

SELECT '{a}' AS x, x LIKE (concat('%', x, '%') AS pat), materialize(x) LIKE pat;
SELECT '{a}' AS x, x LIKE (concat('%', x) AS pat), materialize(x) LIKE pat;
SELECT '{a}' AS x, x LIKE (concat(x, '%') AS pat), materialize(x) LIKE pat;
SELECT '{a}' AS x, x LIKE (x AS pat), materialize(x) LIKE pat;
