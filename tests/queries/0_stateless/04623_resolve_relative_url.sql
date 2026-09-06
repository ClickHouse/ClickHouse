-- Examples from the issue: https://github.com/ClickHouse/ClickHouse/issues/54485
-- Note: a protocol-relative URL takes only the scheme from the base URL.
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com/blog');
SELECT resolveRelativeURL('/image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('//example.com/image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('ftp://example.com/image.gif', 'https://clickhouse.com/blog/');

-- A relative URL with a scheme is returned as is.
SELECT resolveRelativeURL('HTTPS://EXAMPLE.COM/image.gif', 'https://clickhouse.com/blog/');

-- Base URL without a path.
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com');
SELECT resolveRelativeURL('/image.gif', 'https://clickhouse.com');

-- The query string and the fragment of the base URL are dropped when resolving a relative path.
SELECT resolveRelativeURL('image.gif', 'https://clickhouse.com/blog/post?a=1#top');

-- '?query' replaces the query string and the fragment, '#fragment' replaces only the fragment.
SELECT resolveRelativeURL('?a=1', 'https://clickhouse.com/blog/post?b=2#top');
SELECT resolveRelativeURL('#bottom', 'https://clickhouse.com/blog/post?b=2#top');
SELECT resolveRelativeURL('?a=1', 'https://clickhouse.com/blog/post');
SELECT resolveRelativeURL('#bottom', 'https://clickhouse.com/blog/post');

-- Leading './' and '../' segments are collapsed against the base URL's directory.
-- Extra '../' segments that would escape past the root, as well as dot segments in the middle, are kept as is.
SELECT resolveRelativeURL('./image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('../image.gif', 'https://clickhouse.com/blog/2024/');
SELECT resolveRelativeURL('../../image.gif', 'https://clickhouse.com/blog/2024/');
SELECT resolveRelativeURL('../../../image.gif', 'https://clickhouse.com/blog/2024/');
SELECT resolveRelativeURL('.', 'https://clickhouse.com/blog/2024/');
SELECT resolveRelativeURL('..', 'https://clickhouse.com/blog/2024/');
SELECT resolveRelativeURL('a/../image.gif', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('../image.gif', 'https://clickhouse.com');

-- Base URL without a scheme.
SELECT resolveRelativeURL('image.gif', 'clickhouse.com/blog/');
SELECT resolveRelativeURL('/image.gif', 'clickhouse.com/blog/');
SELECT resolveRelativeURL('//example.com/image.gif', 'clickhouse.com/blog/');

-- Empty arguments.
SELECT resolveRelativeURL('', 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', '');
SELECT resolveRelativeURL('', '');

-- Constant and non-constant argument combinations.
SELECT resolveRelativeURL(materialize('image.gif'), 'https://clickhouse.com/blog/');
SELECT resolveRelativeURL('image.gif', materialize('https://clickhouse.com/blog/'));
SELECT resolveRelativeURL(materialize('image.gif'), materialize('https://clickhouse.com/blog/'));

SELECT relative, resolveRelativeURL(relative, base) FROM VALUES('relative String, base String',
    ('image.gif', 'https://clickhouse.com/blog/'),
    ('/image.gif', 'https://clickhouse.com/blog/'),
    ('//example.com/image.gif', 'https://clickhouse.com/blog/'),
    ('ftp://example.com/image.gif', 'https://clickhouse.com/blog/'),
    ('../image.gif', 'https://clickhouse.com/blog/2024/'))
ORDER BY relative;

-- Wrong number of arguments and wrong argument types.
SELECT resolveRelativeURL('image.gif'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT resolveRelativeURL(1, 'https://clickhouse.com/'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT resolveRelativeURL('image.gif', 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
