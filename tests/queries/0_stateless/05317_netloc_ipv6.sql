-- Valid IP literals retain userinfo, brackets and ports through the public SQL wrapper.
SELECT 'constant-userinfo', netloc('http://user:pass@[2001:db8::1]:8080/path') FORMAT CSV;
SELECT 'constant-host', netloc('http://[2001:db8::1]:8080/path') FORMAT CSV;
SELECT 'constant-ipv4-tail', netloc('http://user:pass@[::ffff:192.0.2.1]:8080/') FORMAT CSV;
SELECT 'constant-no-port', netloc('http://[::1]') FORMAT CSV;

-- Exercise materialized `String` columns, including the first authority delimiter.
SELECT case_id, netloc(materialize(url))
FROM values('case_id String, url String',
    ('valid01', 'http://user:pass@[2001:db8::1]:8080/path'),
    ('valid02', 'http://[2001:db8::1]:8080/path'),
    ('valid03', 'http://user:pass@[::ffff:192.0.2.1]:8080/'),
    ('valid04', 'http://[::1]'),
    ('valid05', 'http://[::1]?query'),
    ('valid06', 'http://[::1]#fragment'),
    ('valid07', '//[2001:db8::1]:80/'),
    ('valid08', '[2001:db8::1]:80/'),
    ('valid09', 'user@[2001:db8::1]:80/'),
    ('valid10', 'http://user%3Aname:p%40ss@[2001:db8::1]:80/'),
    ('valid11', 'http://user:pass:word@[2001:db8::1]:80/'),
    ('valid12', 'http://[2001:0db8:0000:0000:0000:0000:0000:0001]:80/'),
    ('valid13', 'http://[64:ff9b::192.0.2.33]/'),
    ('valid14', 'http://[v1.a]:80/'),
    ('valid15', 'http://[::1]/a/b'),
    ('valid16', 'http://[::1]/a#f'),
    ('valid17', 'http://[::1]?q=@x/y'),
    ('valid18', 'http://[::1]/a?x=@y#f'),
    ('valid19', 'http://user:pass@[2001:db8::1]:80/a/b?x=@y#f'),
    ('valid20', 'http://[::1]:8080'),
    ('valid21', 'http://[::1]:8080?query'),
    ('valid22', 'http://[::1]:8080#fragment'),
    ('valid23', 'http://[::1]/')
)
ORDER BY case_id
FORMAT CSV;

-- Literal recognition does not introduce numeric port validation.
-- Restricted characters after the port still terminate the extracted prefix.
SELECT case_id, netloc(materialize(url))
FROM values('case_id String, url String',
    ('port01', 'http://[::1]:80 bad/'),
    ('port02', 'http://[::1]:80]/'),
    ('port03', 'http://user@[::1]:80 bad/'),
    ('port04', 'http://[::1]:abc/')
)
ORDER BY case_id
FORMAT CSV;

-- Preserve legacy results for unrecognized bracketed hosts and ordinary hosts,
-- including malformed userinfo and repeated delimiters outside the new path.
SELECT case_id, netloc(materialize(url))
FROM values('case_id String, url String',
    ('compat01', ''),
    ('compat02', '['),
    ('compat03', ']'),
    ('compat04', 'http://[]/'),
    ('compat05', 'http://user@[bad]/'),
    ('compat06', 'http://user@[::1'),
    ('compat07', 'http://user@[20[01::1]/'),
    ('compat08', 'http://user@[::1]evil.com/'),
    ('compat09', 'http://[::1]:80@evil.com/'),
    ('compat10', 'http://user@[::1@x]/'),
    ('compat11', 'http://example.com/path[::1]'),
    ('compat12', 'http://example.com?q=[::1]'),
    ('compat13', 'http://example.com/path'),
    ('compat14', 'http://user:pass@example.com:8080/path'),
    ('compat15', '//example.com:80/path'),
    ('compat16', 'example.com:80/path'),
    ('compat17', 'http://user@[::1]:80@evil.com/'),
    ('compat18', 'http://user@paypal.com@[::1]:80/'),
    ('compat19', 'http://example.com/path@[::1]'),
    ('compat20', 'http://example.com?q=@[::1]'),
    ('compat21', '[v1.a]:80'),
    ('compat22', '1://[::1]'),
    ('compat23', 'http://[fe80::1%25eth0]/'),
    ('compat24', 'http://example.com/a/b'),
    ('compat25', 'http://example.com/a#f'),
    ('compat26', 'http://example.com?q=@x/y'),
    ('compat27', 'http://example.com/a?x=@y#f'),
    ('compat28', 'http://example.com?q?x'),
    ('compat29', 'http://user:pass@example.com:8080/a/b?x=@y#f')
)
ORDER BY case_id
FORMAT CSV;

SELECT 'nullable-value', netloc(materialize(CAST('http://[::1]' AS Nullable(String)))) FORMAT CSV;
SELECT 'nullable-null', isNull(netloc(materialize(CAST(NULL AS Nullable(String))))) FORMAT CSV;
