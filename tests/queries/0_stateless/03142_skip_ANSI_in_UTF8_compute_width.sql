SELECT format('\x1b[38;2;{0};{1};{2}m█\x1b[0m', 255, 128, 128) AS x FORMAT Pretty;
SELECT 'Hello', format('\x1b[38;2;{0};{1};{2}m█\x1b[0m test \x1b[38;2;{0};{1};{2}m█\x1b[0m', 255, 128, 128) AS x FORMAT Pretty;
