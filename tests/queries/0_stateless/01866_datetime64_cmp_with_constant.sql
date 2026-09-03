CREATE TABLE dt64test
(
    `dt64_column` DateTime64(3),
    `dt_column` DateTime DEFAULT toDateTime(dt64_column)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(dt64_column)
ORDER BY dt64_column;

INSERT INTO dt64test (`dt64_column`) VALUES ('2020-01-13 13:37:00');

SELECT 'dt64 < const dt' FROM dt64test WHERE dt64_column < toDateTime('2020-01-13 13:37:00');
SELECT 'dt64 < dt' FROM dt64test WHERE dt64_column < materialize(toDateTime('2020-01-13 13:37:00'));
SELECT 'dt < const dt64' FROM dt64test WHERE dt_column < toDateTime64('2020-01-13 13:37:00', 3);
SELECT 'dt < dt64' FROM dt64test WHERE dt_column < materialize(toDateTime64('2020-01-13 13:37:00', 3));

SELECT 'dt64 <= const dt' FROM dt64test WHERE dt64_column <= toDateTime('2020-01-13 13:37:00');
SELECT 'dt64 <= dt' FROM dt64test WHERE dt64_column <= materialize(toDateTime('2020-01-13 13:37:00'));
SELECT 'dt <= const dt64' FROM dt64test WHERE dt_column <= toDateTime64('2020-01-13 13:37:00', 3);
SELECT 'dt <= dt64' FROM dt64test WHERE dt_column <= materialize(toDateTime64('2020-01-13 13:37:00', 3));

SELECT 'dt64 = const dt' FROM dt64test WHERE dt64_column = toDateTime('2020-01-13 13:37:00');
SELECT 'dt64 = dt' FROM dt64test WHERE dt64_column = materialize(toDateTime('2020-01-13 13:37:00'));
SELECT 'dt = const dt64' FROM dt64test WHERE dt_column = toDateTime64('2020-01-13 13:37:00', 3);
SELECT 'dt = dt64' FROM dt64test WHERE dt_column = materialize(toDateTime64('2020-01-13 13:37:00', 3));

SELECT 'dt64 >= const dt' FROM dt64test WHERE dt64_column >= toDateTime('2020-01-13 13:37:00');
SELECT 'dt64 >= dt' FROM dt64test WHERE dt64_column >= materialize(toDateTime('2020-01-13 13:37:00'));
SELECT 'dt >= const dt64' FROM dt64test WHERE dt_column >= toDateTime64('2020-01-13 13:37:00', 3);
SELECT 'dt >= dt64' FROM dt64test WHERE dt_column >= materialize(toDateTime64('2020-01-13 13:37:00', 3));

SELECT 'dt64 > const dt' FROM dt64test WHERE dt64_column > toDateTime('2020-01-13 13:37:00');
SELECT 'dt64 > dt' FROM dt64test WHERE dt64_column > materialize(toDateTime('2020-01-13 13:37:00'));
SELECT 'dt > const dt64' FROM dt64test WHERE dt_column > toDateTime64('2020-01-13 13:37:00', 3);
SELECT 'dt > dt64' FROM dt64test WHERE dt_column > materialize(toDateTime64('2020-01-13 13:37:00', 3));

SELECT 'dt64 != const dt' FROM dt64test WHERE dt64_column != toDateTime('2020-01-13 13:37:00');
SELECT 'dt64 != dt' FROM dt64test WHERE dt64_column != materialize(toDateTime('2020-01-13 13:37:00'));
SELECT 'dt != const dt64' FROM dt64test WHERE dt_column != toDateTime64('2020-01-13 13:37:00', 3);
SELECT 'dt != dt64' FROM dt64test WHERE dt_column != materialize(toDateTime64('2020-01-13 13:37:00', 3));

DROP TABLE dt64test;
