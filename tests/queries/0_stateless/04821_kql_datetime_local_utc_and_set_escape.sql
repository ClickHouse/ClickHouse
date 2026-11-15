-- Regressions for `datetime_local_to_utc` converting in the wrong direction, and for the
-- `allow_experimental_kusto_dialect` gate blocking the `SET` escape path of a session that
-- is already in `dialect = 'kusto'`.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- datetime_local_to_utc interprets the wall clock in the given zone --';
print datetime_local_to_utc(datetime(2023-03-16 00:00:00), 'Asia/Shanghai');
print datetime_local_to_utc(datetime(2023-03-16 00:00:00.1234567), 'Asia/Shanghai');
print datetime_local_to_utc(datetime(2023-03-16 00:00:00), 'UTC');
print '-- and datetime_utc_to_local is its inverse --';
print datetime_utc_to_local(datetime(2023-03-15 16:00:00), 'Asia/Shanghai');
print datetime_utc_to_local(datetime_local_to_utc(datetime(2023-03-16 00:00:00), 'Asia/Shanghai'), 'Asia/Shanghai');
print '-- both directions around a DST transition (America/New_York, 2023-03-12) --';
print datetime_local_to_utc(datetime(2023-03-11 12:00:00), 'America/New_York');
print datetime_local_to_utc(datetime(2023-03-12 12:00:00), 'America/New_York');
print datetime_utc_to_local(datetime(2023-03-12 17:00:00), 'America/New_York');

print '-- a plain SET still passes when the gate is off --';
SET allow_experimental_kusto_dialect = 0;
print 1; -- { serverError SUPPORT_IS_DISABLED }
SET allow_experimental_kusto_dialect = 1;
print 2;
SET allow_experimental_kusto_dialect = 0;
SET dialect = 'clickhouse';
SELECT 3;
