SELECT toDateTime('2020-01-02 03:04:05', 'Pacific/Kiritimati') AS x, toStartOfDay(x), toHour(x);
SELECT toDateTime('2020-01-02 03:04:05', 'Africa/El_Aaiun') AS x, toStartOfDay(x), toHour(x);
SELECT toDateTime('2020-01-02 03:04:05', 'Asia/Pyongyang') AS x, toStartOfDay(x), toHour(x);
SELECT toDateTime('2020-01-02 03:04:05', 'Pacific/Kwajalein') AS x, toStartOfDay(x), toHour(x);
SELECT toDateTime('2020-01-02 03:04:05', 'Pacific/Apia') AS x, toStartOfDay(x), toHour(x);
SELECT toDateTime('2020-01-02 03:04:05', 'Pacific/Enderbury') AS x, toStartOfDay(x), toHour(x);
SELECT toDateTime('2020-01-02 03:04:05', 'Pacific/Fakaofo') AS x, toStartOfDay(x), toHour(x);

SELECT toHour(toDateTime(rand(), 'Pacific/Kiritimati') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT toHour(toDateTime(rand(), 'Africa/El_Aaiun') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT toHour(toDateTime(rand(), 'Asia/Pyongyang') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT toHour(toDateTime(rand(), 'Pacific/Kwajalein') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT toHour(toDateTime(rand(), 'Pacific/Apia') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT toHour(toDateTime(rand(), 'Pacific/Enderbury') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT toHour(toDateTime(rand(), 'Pacific/Fakaofo') AS t) AS h, t FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
