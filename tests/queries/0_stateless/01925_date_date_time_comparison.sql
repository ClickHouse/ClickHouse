SELECT toDate('2000-01-01') < toDateTime('2000-01-01 00:00:01', 'Europe/Moscow');
SELECT toDate('2000-01-01') < toDateTime64('2000-01-01 00:00:01', 0, 'Europe/Moscow');
