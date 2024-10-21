with toDateTime('2024-10-16 18:00:30') as t
SELECT toDateTime64(t, 3) + interval 100 milliseconds IN (SELECT t);
