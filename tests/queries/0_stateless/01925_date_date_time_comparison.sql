SELECT toDate('2000-01-01') < toDateTime('2000-01-01 00:00:01', 'Asia/Istanbul');
SELECT toDate('2000-01-01') < toDateTime64('2000-01-01 00:00:01', 0, 'Asia/Istanbul');
