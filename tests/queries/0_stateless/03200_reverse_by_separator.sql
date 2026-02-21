-- Test basic functionality
SELECT reverseBySeparator('www.google.com');
SELECT reverseBySeparator('a/b/c', '/');
SELECT reverseBySeparator('x::y::z', '::');
SELECT reverseBySeparator('a..b', '.');
SELECT reverseBySeparator('.a.b.', '.');
SELECT reverseBySeparator('single');
SELECT reverseBySeparator('');
SELECT reverseBySeparator('a/b/c', '');

-- Test with column input
SELECT reverseBySeparator(domain) FROM (SELECT arrayJoin(['www.google.com', 'mail.yahoo.com', 'clickhouse.com']) AS domain);

-- Edge cases
SELECT reverseBySeparator('...', '.');
SELECT reverseBySeparator('a.b.c', 'x');

-- NULL handling
SELECT reverseBySeparator(NULL);
SELECT reverseBySeparator(NULL, '.');
SELECT reverseBySeparator(NULL, NULL);
SELECT reverseBySeparator('test', NULL);

-- Empty and special cases
SELECT reverseBySeparator('abc', 'abc');
SELECT reverseBySeparator('abcabc', 'abc');
SELECT reverseBySeparator('test', '');
SELECT reverseBySeparator('', '.');

-- Multi-character separators
SELECT reverseBySeparator('one::two::three', '::');
SELECT reverseBySeparator('start--middle--end', '--');
SELECT reverseBySeparator('a|||b|||c', '|||');

-- Performance test with repeated patterns
SELECT reverseBySeparator('a.b.c.d.e.f.g.h.i.j');
SELECT reverseBySeparator('1/2/3/4/5/6/7/8/9/10', '/');