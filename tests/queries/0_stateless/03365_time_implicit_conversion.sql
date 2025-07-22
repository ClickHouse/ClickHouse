SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS dt;

CREATE TABLE dt
(
    `time` Time,
    `event_id` UInt8
)
ENGINE = TinyLog;

INSERT INTO dt VALUES ('100:00:00', 1), (12453, 3);

SELECT * FROM dt WHERE time = '100:00:00';
