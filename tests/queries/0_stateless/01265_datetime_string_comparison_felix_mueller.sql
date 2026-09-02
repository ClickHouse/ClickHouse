DROP TABLE IF EXISTS tztest;

CREATE TABLE tztest
(
    timeBerlin DateTime('Europe/Berlin'), 
    timeLA DateTime('America/Los_Angeles')
)
ENGINE = Memory;

INSERT INTO tztest (timeBerlin, timeLA) VALUES ('2019-05-06 12:00:00', '2019-05-06 12:00:00');

SELECT
    toUnixTimestamp(timeBerlin),
    toUnixTimestamp(timeLA)
FROM tztest;

SELECT 1
FROM tztest
WHERE timeBerlin = '2019-05-06 12:00:00';

SELECT 1
FROM tztest
WHERE timeLA = '2019-05-06 12:00:00';

SELECT 1
FROM tztest
WHERE '2019-05-06 12:00:00' = timeBerlin;

DROP TABLE tztest;
