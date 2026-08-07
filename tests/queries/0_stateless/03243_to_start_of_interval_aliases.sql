SELECT date_bin(toDate('2023-10-09'), toIntervalYear(1), toDate('2022-02-01'));
SELECT date_bin(toDate('2023-10-09'), toIntervalYear(1));
SELECT date_BIN(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1), toDateTime('2022-02-01 09:08:07'));
SELECT date_BIN(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1));
SELECT time_bucket(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1), toDateTime('2022-02-01 09:08:07'));
SELECT time_bucket(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1));
SELECT TIME_bucket(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1), toDateTime('2022-02-01 09:08:07'));
SELECT TIME_bucket(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1));
