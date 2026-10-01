-- Outer construction settings are refused rather than silently ignored.
EXPLAIN TEXT (SELECT 1) SETTINGS page = 3; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT (SELECT 1) SETTINGS filter = '0'; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT (SELECT 1) SETTINGS limit = 5; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT (SELECT 1) SETTINGS limit = DEFAULT; -- { serverError BAD_ARGUMENTS }
EXPLAIN TEXT SELECT 1 ONELINE SETTINGS offset = 2; -- { serverError BAD_ARGUMENTS }

-- Construction settings inside the source stay preserved text.
EXPLAIN TEXT (SELECT 1 SETTINGS filter = '0') ONELINE;
EXPLAIN TEXT (SELECT 1 LIMIT 10 SETTINGS page = 2) PAGE 2, ONELINE;
