SET session_timezone='Europe/Amsterdam';
SELECT toString(toDateTime('2022-01-01 12:13:14'), CAST('UTC', 'Nullable(String)'));