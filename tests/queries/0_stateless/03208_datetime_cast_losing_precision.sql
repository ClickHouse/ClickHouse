WITH toDateTime('2024-10-16 18:00:30') as t
SELECT toDateTime64(t, 3) + interval 100 milliseconds IN (SELECT t) settings transform_null_in=0;

WITH toDateTime('2024-10-16 18:00:30') as t
SELECT toDateTime64(t, 3) + interval 100 milliseconds IN (SELECT t) settings transform_null_in=1;

WITH toDateTime('1970-01-01 00:00:01') as t
SELECT toDateTime64(t, 3) + interval 100 milliseconds IN (now(), Null) settings transform_null_in=1;

WITH toDateTime('1970-01-01 00:00:01') as t
SELECT toDateTime64(t, 3) + interval 100 milliseconds IN (now(), Null) settings transform_null_in=0;

WITH toDateTime('1970-01-01 00:00:01') as t,
    arrayJoin([Null, toDateTime64(t, 3) + interval 100 milliseconds]) as x
SELECT x IN (now(), Null) settings transform_null_in=0;

WITH toDateTime('1970-01-01 00:00:01') as t,
    arrayJoin([Null, toDateTime64(t, 3) + interval 100 milliseconds]) as x
SELECT x IN (now(), Null) settings transform_null_in=1;

WITH toDateTime('2024-10-16 18:00:30') as t
SELECT (
    SELECT
        toDateTime64(t, 3) + interval 100 milliseconds,
        toDateTime64(t, 3) + interval 101 milliseconds
)
IN (
    SELECT
        t,
        t
) SETTINGS transform_null_in=0;

WITH toDateTime('2024-10-16 18:00:30') as t
SELECT (
    SELECT
        toDateTime64(t, 3) + interval 100 milliseconds,
        toDateTime64(t, 3) + interval 101 milliseconds
)
IN (
    SELECT
            t,
            t
) SETTINGS transform_null_in=1;
