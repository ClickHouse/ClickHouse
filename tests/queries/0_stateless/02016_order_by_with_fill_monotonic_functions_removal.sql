SELECT toStartOfMinute(some_time) AS ts
FROM
(
    SELECT toDateTime('2021-07-07 15:21:05') AS some_time
)
ORDER BY ts ASC WITH FILL FROM toDateTime('2021-07-07 15:21:00') TO toDateTime('2021-07-07 15:21:15') STEP 5;
