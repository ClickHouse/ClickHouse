SET enable_time_time64_type = 1;

CREATE TABLE dt
(
    `time` Time,
    `event_id` UInt8
)
ENGINE = TinyLog;

INSERT INTO dt VALUES ('100:00:00', 1), (12453, 3);

SELECT max(time)
FROM dt;

SELECT min(time)
FROM dt;

SELECT avg(time)
FROM dt; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
