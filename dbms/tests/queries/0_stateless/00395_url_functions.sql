SELECT protocol('http://example.com') AS Scheme;
SELECT protocol('https://example.com/') AS Scheme;
SELECT protocol('svn+ssh://example.com?q=hello%20world') AS Scheme;
SELECT protocol('ftp!://example.com/') AS Scheme;
