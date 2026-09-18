-- URL hierarchy parsing must accept lowercase letters and digits at the
-- boundaries of the protocol scan.
SELECT
    URLHierarchy('abc://example.com/a/b'),
    URLHierarchy('zbc://example.com/a/b'),
    URLHierarchy('bc0d://example.com/a/b'),
    URLHierarchy('bc9d://example.com/a/b');

SELECT
    URLPathHierarchy('abc://example.com/a/b'),
    URLPathHierarchy('zbc://example.com/a/b'),
    URLPathHierarchy('bc0d://example.com/a/b'),
    URLPathHierarchy('bc9d://example.com/a/b');

SELECT
    URLHash('abc://example.com/a/b', 0) = URLHash('abc://example.com/'),
    URLHash('zbc://example.com/a/b', 0) = URLHash('zbc://example.com/'),
    URLHash('bc0d://example.com/a/b', 0) = URLHash('bc0d://example.com/'),
    URLHash('bc9d://example.com/a/b', 0) = URLHash('bc9d://example.com/');
