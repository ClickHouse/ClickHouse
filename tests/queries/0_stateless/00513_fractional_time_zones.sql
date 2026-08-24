WITH toDateTime(1509138000) + number * 300 AS t SELECT toHour(t, 'Asia/Kolkata') AS h, toString(toStartOfHour(t, 'Asia/Kolkata'), 'Asia/Kolkata') AS h_start FROM system.numbers LIMIT 12;
