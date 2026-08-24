SET use_legacy_to_time = 0;

-- TIME AND TIME
-- Both positive
SELECT toTime(12) > toTime(13);
SELECT toTime(13) > toTime(12);
SELECT toTime(12) > toTime(12);

SELECT toTime(12) < toTime(13);
SELECT toTime(13) < toTime(12);
SELECT toTime(12) < toTime(12);

SELECT toTime(12) == toTime(13);
SELECT toTime(13) == toTime(12);
SELECT toTime(12) == toTime(12);

-- Both negative
SELECT toTime(-12) > toTime(-13);
SELECT toTime(-13) > toTime(-12);
SELECT toTime(-12) > toTime(-12);

SELECT toTime(-12) < toTime(-13);
SELECT toTime(-13) < toTime(-12);
SELECT toTime(-12) < toTime(-12);

SELECT toTime(-12) == toTime(-13);
SELECT toTime(-13) == toTime(-12);
SELECT toTime(-12) == toTime(-12);

-- Left negative
SELECT toTime(-12) > toTime(13);
SELECT toTime(-13) > toTime(12);
SELECT toTime(-12) > toTime(12);

SELECT toTime(-12) < toTime(13);
SELECT toTime(-13) < toTime(12);
SELECT toTime(-12) < toTime(12);

SELECT toTime(-12) == toTime(13);
SELECT toTime(-13) == toTime(12);
SELECT toTime(-12) == toTime(12);

-- Right negative
SELECT toTime(12) > toTime(-13);
SELECT toTime(13) > toTime(-12);
SELECT toTime(12) > toTime(-12);

SELECT toTime(12) < toTime(-13);
SELECT toTime(13) < toTime(-12);
SELECT toTime(12) < toTime(-12);

SELECT toTime(12) == toTime(-13);
SELECT toTime(13) == toTime(-12);
SELECT toTime(12) == toTime(-12);

-- TIME64 AND TIME64
-- Both positive
SELECT toTime64(12, 2) > toTime64(13, 2);
SELECT toTime64(13, 2) > toTime64(12, 2);
SELECT toTime64(12, 2) > toTime64(12, 2);

SELECT toTime64(12, 2) < toTime64(13, 2);
SELECT toTime64(13, 2) < toTime64(12, 2);
SELECT toTime64(12, 2) < toTime64(12, 2);

SELECT toTime64(12, 2) == toTime64(13, 2);
SELECT toTime64(13, 2) == toTime64(12, 2);
SELECT toTime64(12, 2) == toTime64(12, 2);

-- Both negative
SELECT toTime64(-12, 2) > toTime64(-13, 2);
SELECT toTime64(-13, 2) > toTime64(-12, 2);
SELECT toTime64(-12, 2) > toTime64(-12, 2);

SELECT toTime64(-12, 2) < toTime64(-13, 2);
SELECT toTime64(-13, 2) < toTime64(-12, 2);
SELECT toTime64(-12, 2) < toTime64(-12, 2);

SELECT toTime64(-12, 2) == toTime64(-13, 2);
SELECT toTime64(-13, 2) == toTime64(-12, 2);
SELECT toTime64(-12, 2) == toTime64(-12, 2);

-- Left negative
SELECT toTime64(-12, 2) > toTime64(13, 2);
SELECT toTime64(-13, 2) > toTime64(12, 2);
SELECT toTime64(-12, 2) > toTime64(12, 2);

SELECT toTime64(-12, 2) < toTime64(13, 2);
SELECT toTime64(-13, 2) < toTime64(12, 2);
SELECT toTime64(-12, 2) < toTime64(12, 2);

SELECT toTime64(-12, 2) == toTime64(13, 2);
SELECT toTime64(-13, 2) == toTime64(12, 2);
SELECT toTime64(-12, 2) == toTime64(12, 2);

-- Right negative
SELECT toTime64(12, 2) > toTime64(-13, 2);
SELECT toTime64(13, 2) > toTime64(-12, 2);
SELECT toTime64(12, 2) > toTime64(-12, 2);

SELECT toTime64(12, 2) < toTime64(-13, 2);
SELECT toTime64(13, 2) < toTime64(-12, 2);
SELECT toTime64(12, 2) < toTime64(-12, 2);

SELECT toTime64(12, 2) == toTime64(-13, 2);
SELECT toTime64(13, 2) == toTime64(-12, 2);
SELECT toTime64(12, 2) == toTime64(-12, 2);


-- Different fractional size
SELECT toTime64(12, 2) > toTime64(13, 3);
SELECT toTime64(13, 2) > toTime64(12, 3);
SELECT toTime64(12, 2) > toTime64(12, 3);

SELECT toTime64(12, 2) < toTime64(13, 3);
SELECT toTime64(13, 2) < toTime64(12, 3);
SELECT toTime64(12, 2) < toTime64(12, 3);

SELECT toTime64(12, 2) == toTime64(13, 3);
SELECT toTime64(13, 2) == toTime64(12, 3);
SELECT toTime64(12, 2) == toTime64(12, 3);

-- TIME AND TIME64
-- Both positive
SELECT toTime(12) > toTime64(13, 2);
SELECT toTime(13) > toTime64(12, 2);
SELECT toTime(12) > toTime64(12, 2);

SELECT toTime(12) < toTime64(13, 2);
SELECT toTime(13) < toTime64(12, 2);
SELECT toTime(12) < toTime64(12, 2);

SELECT toTime(12) == toTime64(13, 2);
SELECT toTime(13) == toTime64(12, 2);
SELECT toTime(12) == toTime64(12, 2);

-- Both negative
SELECT toTime(-12) > toTime64(-13, 2);
SELECT toTime(-13) > toTime64(-12, 2);
SELECT toTime(-12) > toTime64(-12, 2);

SELECT toTime(-12) < toTime64(-13, 2);
SELECT toTime(-13) < toTime64(-12, 2);
SELECT toTime(-12) < toTime64(-12, 2);

SELECT toTime(-12) == toTime64(-13, 2);
SELECT toTime(-13) == toTime64(-12, 2);
SELECT toTime(-12) == toTime64(-12, 2);

-- Left negative
SELECT toTime(-12) > toTime64(13, 2);
SELECT toTime(-13) > toTime64(12, 2);
SELECT toTime(-12) > toTime64(12, 2);

SELECT toTime(-12) < toTime64(13, 2);
SELECT toTime(-13) < toTime64(12, 2);
SELECT toTime(-12) < toTime64(12, 2);

SELECT toTime(-12) == toTime64(13, 2);
SELECT toTime(-13) == toTime64(12, 2);
SELECT toTime(-12) == toTime64(12, 2);

-- Right negative
SELECT toTime(12) > toTime64(-13, 2);
SELECT toTime(13) > toTime64(-12, 2);
SELECT toTime(12) > toTime64(-12, 2);

SELECT toTime(12) < toTime64(-13, 2);
SELECT toTime(13) < toTime64(-12, 2);
SELECT toTime(12) < toTime64(-12, 2);

SELECT toTime(12) == toTime64(-13, 2);
SELECT toTime(13) == toTime64(-12, 2);
SELECT toTime(12) == toTime64(-12, 2);
