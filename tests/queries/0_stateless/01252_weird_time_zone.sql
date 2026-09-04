SELECT 'Pacific/Kiritimati', toDateTime('2020-01-02 03:04:05', 'Pacific/Kiritimati') AS x, toStartOfDay(x), toHour(x);
SELECT 'Africa/El_Aaiun', toDateTime('2020-01-02 03:04:05', 'Africa/El_Aaiun') AS x, toStartOfDay(x), toHour(x);
SELECT 'Asia/Pyongyang', toDateTime('2020-01-02 03:04:05', 'Asia/Pyongyang') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Kwajalein', toDateTime('2020-01-02 03:04:05', 'Pacific/Kwajalein') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Apia', toDateTime('2020-01-02 03:04:05', 'Pacific/Apia') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Enderbury', toDateTime('2020-01-02 03:04:05', 'Pacific/Enderbury') AS x, toStartOfDay(x), toHour(x);
SELECT 'Pacific/Fakaofo', toDateTime('2020-01-02 03:04:05', 'Pacific/Fakaofo') AS x, toStartOfDay(x), toHour(x);

SELECT 'Pacific/Kiritimati', rand() as r, toHour(toDateTime(r, 'Pacific/Kiritimati') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Africa/El_Aaiun', rand() as r, toHour(toDateTime(r, 'Africa/El_Aaiun') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Asia/Pyongyang', rand() as r, toHour(toDateTime(r, 'Asia/Pyongyang') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Kwajalein', rand() as r, toHour(toDateTime(r, 'Pacific/Kwajalein') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Apia', rand() as r, toHour(toDateTime(r, 'Pacific/Apia') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Enderbury', rand() as r, toHour(toDateTime(r, 'Pacific/Enderbury') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
SELECT 'Pacific/Fakaofo', rand() as r, toHour(toDateTime(r, 'Pacific/Fakaofo') AS t) AS h, t, toTypeName(t) FROM numbers(1000000) WHERE h < 0 OR h > 23 ORDER BY h LIMIT 1 BY h;
