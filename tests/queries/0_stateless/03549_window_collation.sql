-- Tags: no-fasttest
-- no-fasttest - collations support is disabled for fasttest build

SELECT rank() OVER (ORDER BY c0) FROM (SELECT '1' c0) v0 QUALIFY rank() OVER (ORDER BY c0 COLLATE 'vi') > 0; -- { serverError BAD_COLLATION }
SELECT rank() OVER (ORDER BY c0 COLLATE 'vi') FROM (SELECT '1' c0) v0 QUALIFY rank() OVER (ORDER BY c0 COLLATE 'vi') > 0;
SELECT rank() OVER (ORDER BY c0) FROM (SELECT '1' c0) v0 QUALIFY rank() OVER (ORDER BY c0) > 0;
