SELECT resolveRelativeURL('ftp://example.com/image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', '');
SELECT resolveRelativeURL('/image.gif', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL('/image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('/', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('//example.com/image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('//', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('///', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('///image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('////image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com');
SELECT resolveRelativeURL(':/image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL(':', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL(':', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL('Δδ', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL('http:image.gif', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL('https:image.gif', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL(materialize('image.gif'), materialize('https://clickhouse.com/blog/'));
SELECT resolveRelativeURL(materialize('image.gif'), 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', materialize('https://clickhouse.com/blog/'));
-- The following are from RFC 3986 section 5.4
WITH exam(relative_url, base_url, target_url) AS
(         SELECT 'g:h', 'http://a/b/c/d;p?q', 'g:h'
UNION ALL SELECT 'g', 'http://a/b/c/d;p?q', 'http://a/b/c/g'
UNION ALL SELECT './g', 'http://a/b/c/d;p?q', 'http://a/b/c/g'
UNION ALL SELECT 'g/', 'http://a/b/c/d;p?q', 'http://a/b/c/g/'
UNION ALL SELECT '/g', 'http://a/b/c/d;p?q', 'http://a/g'
UNION ALL SELECT '//g', 'http://a/b/c/d;p?q', 'http://g'
UNION ALL SELECT '?y', 'http://a/b/c/d;p?q', 'http://a/b/c/d;p?y'
UNION ALL SELECT 'g?y', 'http://a/b/c/d;p?q', 'http://a/b/c/g?y'
UNION ALL SELECT '#s', 'http://a/b/c/d;p?q', 'http://a/b/c/d;p?q#s'
UNION ALL SELECT 'g#s', 'http://a/b/c/d;p?q', 'http://a/b/c/g#s'
UNION ALL SELECT 'g?y#s', 'http://a/b/c/d;p?q', 'http://a/b/c/g?y#s'
UNION ALL SELECT ';x', 'http://a/b/c/d;p?q', 'http://a/b/c/;x'
UNION ALL SELECT 'g;x', 'http://a/b/c/d;p?q', 'http://a/b/c/g;x'
UNION ALL SELECT 'g;x?y#s', 'http://a/b/c/d;p?q', 'http://a/b/c/g;x?y#s'
UNION ALL SELECT '', 'http://a/b/c/d;p?q', 'http://a/b/c/d;p?q'
UNION ALL SELECT '.', 'http://a/b/c/d;p?q', 'http://a/b/c/'
UNION ALL SELECT './', 'http://a/b/c/d;p?q', 'http://a/b/c/'
UNION ALL SELECT '..', 'http://a/b/c/d;p?q', 'http://a/b/'
UNION ALL SELECT '../', 'http://a/b/c/d;p?q', 'http://a/b/'
UNION ALL SELECT '../g', 'http://a/b/c/d;p?q', 'http://a/b/g'
UNION ALL SELECT '../..', 'http://a/b/c/d;p?q', 'http://a/'
UNION ALL SELECT '../../', 'http://a/b/c/d;p?q', 'http://a/'
UNION ALL SELECT '../../g', 'http://a/b/c/d;p?q', 'http://a/g'
UNION ALL SELECT '../../../g', 'http://a/b/c/d;p?q', 'http://a/g'
UNION ALL SELECT '../../../../g', 'http://a/b/c/d;p?q', 'http://a/g'
UNION ALL SELECT '/./g', 'http://a/b/c/d;p?q', 'http://a/g'
UNION ALL SELECT '/../g', 'http://a/b/c/d;p?q', 'http://a/g'
UNION ALL SELECT 'g.', 'http://a/b/c/d;p?q', 'http://a/b/c/g.'
UNION ALL SELECT '.g', 'http://a/b/c/d;p?q', 'http://a/b/c/.g'
UNION ALL SELECT 'g..', 'http://a/b/c/d;p?q', 'http://a/b/c/g..'
UNION ALL SELECT '..g', 'http://a/b/c/d;p?q', 'http://a/b/c/..g'
UNION ALL SELECT './../g', 'http://a/b/c/d;p?q', 'http://a/b/g'
UNION ALL SELECT './g/.', 'http://a/b/c/d;p?q', 'http://a/b/c/g/'
UNION ALL SELECT 'g/./h', 'http://a/b/c/d;p?q', 'http://a/b/c/g/h'
UNION ALL SELECT 'g/../h', 'http://a/b/c/d;p?q', 'http://a/b/c/h'
UNION ALL SELECT 'g;x=1/./y', 'http://a/b/c/d;p?q', 'http://a/b/c/g;x=1/y'
UNION ALL SELECT 'g;x=1/../y', 'http://a/b/c/d;p?q', 'http://a/b/c/y'
UNION ALL SELECT 'g?y/./x', 'http://a/b/c/d;p?q', 'http://a/b/c/g?y/./x'
UNION ALL SELECT 'g?y/../x', 'http://a/b/c/d;p?q', 'http://a/b/c/g?y/../x'
UNION ALL SELECT 'g#s/./x', 'http://a/b/c/d;p?q', 'http://a/b/c/g#s/./x'
UNION ALL SELECT 'g#s/../x', 'http://a/b/c/d;p?q', 'http://a/b/c/g#s/../x'
UNION ALL SELECT 'http:g', 'http://a/b/c/d;p?q', 'http:g'
)
SELECT count(1) FROM exam WHERE resolveRelativeURL(relative_url, base_url) = target_url; 
