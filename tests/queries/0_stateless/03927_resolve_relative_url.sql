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
(         SELECT 'g:h' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'g:h' AS target_url
UNION ALL SELECT 'g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g' AS target_url
UNION ALL SELECT './g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g' AS target_url
UNION ALL SELECT 'g/' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g/' AS target_url
UNION ALL SELECT '/g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/g' AS target_url
UNION ALL SELECT '//g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://g' AS target_url
UNION ALL SELECT '?y' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/d;p?y' AS target_url
UNION ALL SELECT 'g?y' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g?y' AS target_url
UNION ALL SELECT '#s' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/d;p?q#s' AS target_url
UNION ALL SELECT 'g#s' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g#s' AS target_url
UNION ALL SELECT 'g?y#s' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g?y#s' AS target_url
UNION ALL SELECT ';x' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/;x' AS target_url
UNION ALL SELECT 'g;x' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g;x' AS target_url
UNION ALL SELECT 'g;x?y#s' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g;x?y#s' AS target_url
UNION ALL SELECT '' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/d;p?q' AS target_url
UNION ALL SELECT '.' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/' AS target_url
UNION ALL SELECT './' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/' AS target_url
UNION ALL SELECT '..' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/' AS target_url
UNION ALL SELECT '../' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/' AS target_url
UNION ALL SELECT '../g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/g' AS target_url
UNION ALL SELECT '../..' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/' AS target_url
UNION ALL SELECT '../../' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/' AS target_url
UNION ALL SELECT '../../g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/g' AS target_url
UNION ALL SELECT '../../../g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/g' AS target_url
UNION ALL SELECT '../../../../g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/g' AS target_url
UNION ALL SELECT '/./g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/g' AS target_url
UNION ALL SELECT '/../g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/g' AS target_url
UNION ALL SELECT 'g.' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g.' AS target_url
UNION ALL SELECT '.g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/.g' AS target_url
UNION ALL SELECT 'g..' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g..' AS target_url
UNION ALL SELECT '..g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/..g' AS target_url
UNION ALL SELECT './../g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/g' AS target_url
UNION ALL SELECT './g/.' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g/' AS target_url
UNION ALL SELECT 'g/./h' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g/h' AS target_url
UNION ALL SELECT 'g/../h' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/h' AS target_url
UNION ALL SELECT 'g;x=1/./y' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g;x=1/y' AS target_url
UNION ALL SELECT 'g;x=1/../y' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/y' AS target_url
UNION ALL SELECT 'g?y/./x' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g?y/./x' AS target_url
UNION ALL SELECT 'g?y/../x' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g?y/../x' AS target_url
UNION ALL SELECT 'g#s/./x' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g#s/./x' AS target_url
UNION ALL SELECT 'g#s/../x' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http://a/b/c/g#s/../x' AS target_url
UNION ALL SELECT 'http:g' AS relative_url, 'http://a/b/c/d;p?q' AS base_url, 'http:g' AS target_url
)
SELECT count(1) FROM exam WHERE resolveRelativeURL(relative_url, base_url) = target_url; 
