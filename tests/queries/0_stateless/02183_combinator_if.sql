SELECT anyIf(toNullable('Hello'), arrayJoin([1, NULL]) = 0);

SELECT anyIf(toNullable('Hello'), arrayJoin([1, 1]) = 0);
SELECT anyIf(toNullable('Hello'), arrayJoin([1, 0]) = 0);
SELECT anyIf(toNullable('Hello'), arrayJoin([0, 1]) = 0);
SELECT anyIf(toNullable('Hello'), arrayJoin([0, 0]) = 0);

SELECT anyIf('Hello', arrayJoin([1, NULL]) = 0);
SELECT anyIf('Hello', arrayJoin([1, NULL]) = 1);
SELECT anyIf('Hello', arrayJoin([1, NULL]) IS NULL);

SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, NULL]) = 0) FROM numbers(2) GROUP BY number ORDER BY number;
SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, NULL, 0]) = 0) FROM numbers(2) GROUP BY number ORDER BY number;

SELECT number, anyIf('Hello', arrayJoin([1, NULL]) = 0) FROM numbers(2) GROUP BY number ORDER BY number;
SELECT number, anyIf('Hello', arrayJoin([1, NULL, 0]) = 0) FROM numbers(2) GROUP BY number ORDER BY number;

SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, 1]) = 0) FROM numbers(2) GROUP BY number ORDER BY number;
SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, 0]) = 0) FROM numbers(2) GROUP BY number ORDER BY number;


SELECT anyIf(toNullable('Hello'), arrayJoin([1, NULL]) = 0) FROM remote('127.0.0.{1,2}', system.one);

SELECT anyIf(toNullable('Hello'), arrayJoin([1, 1]) = 0) FROM remote('127.0.0.{1,2}', system.one);
SELECT anyIf(toNullable('Hello'), arrayJoin([1, 0]) = 0) FROM remote('127.0.0.{1,2}', system.one);
SELECT anyIf(toNullable('Hello'), arrayJoin([0, 1]) = 0) FROM remote('127.0.0.{1,2}', system.one);
SELECT anyIf(toNullable('Hello'), arrayJoin([0, 0]) = 0) FROM remote('127.0.0.{1,2}', system.one);

SELECT anyIf('Hello', arrayJoin([1, NULL]) = 0) FROM remote('127.0.0.{1,2}', system.one);
SELECT anyIf('Hello', arrayJoin([1, NULL]) = 1) FROM remote('127.0.0.{1,2}', system.one);
SELECT anyIf('Hello', arrayJoin([1, NULL]) IS NULL) FROM remote('127.0.0.{1,2}', system.one);

SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, NULL]) = 0) FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY number ORDER BY number;
SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, NULL, 0]) = 0) FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY number ORDER BY number;

SELECT number, anyIf('Hello', arrayJoin([1, NULL]) = 0) FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY number ORDER BY number;
SELECT number, anyIf('Hello', arrayJoin([1, NULL, 0]) = 0) FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY number ORDER BY number;

SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, 1]) = 0) FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY number ORDER BY number;
SELECT number, anyIf(toNullable('Hello'), arrayJoin([1, 0]) = 0) FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY number ORDER BY number;
