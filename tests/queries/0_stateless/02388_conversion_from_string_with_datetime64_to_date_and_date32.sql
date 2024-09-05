SELECT toDate('2022-08-22 01:02:03');
SELECT toDate32('2022-08-22 01:02:03');

SELECT toDate('2022-08-22 01:02:03.1');
SELECT toDate32('2022-08-22 01:02:03.1');

SELECT toDate('2022-08-22 01:02:03.123456');
SELECT toDate32('2022-08-22 01:02:03.123456');

SELECT toDate('2022-08-22T01:02:03');
SELECT toDate32('2022-08-22T01:02:03');

SELECT toDate('2022-08-22T01:02:03.1');
SELECT toDate32('2022-08-22T01:02:03.1');

SELECT toDate('2022-08-22T01:02:03.123456');
SELECT toDate32('2022-08-22T01:02:03.123456');


SELECT toDate('2022-08-22+01:02:03'); -- { serverError 6 }
SELECT toDate32('2022-08-22+01:02:03'); -- { serverError 6 }

SELECT toDate('2022-08-22 01:02:0'); -- { serverError 6 }
SELECT toDate32('2022-08-22 01:02:0'); -- { serverError 6 }

SELECT toDate('2022-08-22 01:02:03.'); -- { serverError 6 }
SELECT toDate32('2022-08-22 01:02:03.'); -- { serverError 6 }

SELECT toDate('2022-08-22 01:02:03.111a'); -- { serverError 6 }
SELECT toDate32('2022-08-22 01:02:03.2b'); -- { serverError 6 }

SELECT toDate('2022-08-22 01:02:03.a'); -- { serverError 6 }
SELECT toDate32('2022-08-22 01:02:03.b'); -- { serverError 6 }
