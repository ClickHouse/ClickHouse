SELECT format('\x1b[38;2;{0};{1};{2}m█\x1b[0m', 255, 128, 128) AS x;
SELECT format('\x1b[38;2;{0};{1};{2}m█ test \x1b[0m', 255, 128, 128) AS x;
SELECT format('\x1b[38;2;{0};{1};{2}m█\x1b[0m test', 255, 128, 128) AS x;
SELECT format('test \x1b[38;2;{0};{1};{2}m█\x1b[0m', 255, 128, 128) AS x;
SELECT format('\x1b[38;2;{0};{1};{2}m█\x1b[0m test \x1b[38;2;{0};{1};{2}m█\x1b[0m', 255, 128, 128) AS x;
SELECT visibleWidth('0};{1};{2}m█');