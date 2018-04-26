SELECT '====SCHEMA====';
SELECT protocol('http://example.com') AS Scheme;
SELECT protocol('https://example.com/') AS Scheme;
SELECT protocol('svn+ssh://example.com?q=hello%20world') AS Scheme;
SELECT protocol('ftp!://example.com/') AS Scheme;
SELECT protocol('http://127.0.0.1:443/') AS Scheme;
SELECT protocol('//127.0.0.1:443/') AS Scheme;

SELECT '====HOST====';
SELECT domain('http://paul@www.example.com:80/') AS Host;
SELECT domain('http:/paul/example/com') AS Host;
SELECT domain('http://www.example.com?q=4') AS Host;
SELECT domain('http://127.0.0.1:443/') AS Host;
SELECT domain('//www.example.com') AS Host;
SELECT domain('//paul@www.example.com') AS Host;
SELECT domainWithoutWWW('//paul@www.example.com') AS Host;
SELECT domainWithoutWWW('http://paul@www.example.com:80/') AS Host;


SELECT '====DOMAIN====';
SELECT topLevelDomain('http://paul@www.example.com:80/') AS Domain;
SELECT topLevelDomain('http://127.0.0.1:443/') AS Domain;
SELECT topLevelDomain('svn+ssh://example.ru?q=hello%20world') AS Domain;
SELECT topLevelDomain('svn+ssh://example.ru.?q=hello%20world') AS Domain;
SELECT topLevelDomain('//www.example.com') AS Domain;

SELECT '====PATH====';
SELECT decodeURLComponent('%D0%9F');
SELECT decodeURLComponent('%D%9');
SELECT decodeURLComponent(pathFull('//127.0.0.1/?query=hello%20world+foo%2Bbar')) AS Path;
SELECT decodeURLComponent(pathFull('http://127.0.0.1/?query=hello%20world+foo%2Bbar')) AS Path;
SELECT decodeURLComponent(materialize(pathFull('http://127.0.0.1/?query=hello%20world+foo%2Bbar'))) AS Path;
SELECT decodeURLComponent(materialize(pathFull('//127.0.0.1/?query=hello%20world+foo%2Bbar'))) AS Path;
SELECT path('http://127.0.0.1') AS Path;
SELECT path('http://127.0.0.1/a/b/c') AS Path;
SELECT path('http://127.0.0.1:443/a/b/c') AS Path;
SELECT path('http://paul@127.0.0.1:443/a/b/c') AS Path;
SELECT path('//paul@127.0.0.1:443/a/b/c') AS Path;

SELECT '====QUERY STRING====';
SELECT decodeURLComponent(queryString('http://127.0.0.1/'));
SELECT decodeURLComponent(queryString('http://127.0.0.1/?'));
SELECT decodeURLComponent(queryString('http://127.0.0.1/?query=hello%20world+foo%2Bbar'));
SELECT decodeURLComponent(queryString('http://127.0.0.1:443/?query=hello%20world+foo%2Bbar'));
SELECT decodeURLComponent(queryString('http://paul@127.0.0.1:443/?query=hello%20world+foo%2Bbar'));
SELECT decodeURLComponent(queryString('//paul@127.0.0.1:443/?query=hello%20world+foo%2Bbar'));

SELECT '====FRAGMENT====';
SELECT decodeURLComponent(fragment('http://127.0.0.1/?query=hello%20world+foo%2Bbar'));
SELECT decodeURLComponent(fragment('http://127.0.0.1/?query=hello%20world+foo%2Bbar#'));
SELECT decodeURLComponent(fragment('http://127.0.0.1/?query=hello%20world+foo%2Bbar#a=b'));
SELECT decodeURLComponent(fragment('http://paul@127.0.0.1/?query=hello%20world+foo%2Bbar#a=b'));
SELECT decodeURLComponent(fragment('//paul@127.0.0.1/?query=hello%20world+foo%2Bbar#a=b'));

SELECT '====QUERY STRING AND FRAGMENT====';
SELECT decodeURLComponent(queryStringAndFragment('http://127.0.0.1/'));
SELECT decodeURLComponent(queryStringAndFragment('http://127.0.0.1/?'));
SELECT decodeURLComponent(queryStringAndFragment('http://127.0.0.1/?query=hello%20world+foo%2Bbar'));
SELECT decodeURLComponent(queryStringAndFragment('http://127.0.0.1/?query=hello%20world+foo%2Bbar#a=b'));
SELECT decodeURLComponent(queryStringAndFragment('http://paul@127.0.0.1/?query=hello%20world+foo%2Bbar#a=b'));
SELECT decodeURLComponent(queryStringAndFragment('//paul@127.0.0.1/?query=hello%20world+foo%2Bbar#a=b'));

SELECT '====CUT TO FIRST SIGNIFICANT SUBDOMAIN====';
SELECT cutToFirstSignificantSubdomain('http://www.example.com');
SELECT cutToFirstSignificantSubdomain('http://www.example.com:1234');
SELECT cutToFirstSignificantSubdomain('http://www.example.com/a/b/c');
SELECT cutToFirstSignificantSubdomain('http://www.example.com/a/b/c?a=b');
SELECT cutToFirstSignificantSubdomain('http://www.example.com/a/b/c?a=b#d=f');
SELECT cutToFirstSignificantSubdomain('http://paul@www.example.com/a/b/c?a=b#d=f');
SELECT cutToFirstSignificantSubdomain('//paul@www.example.com/a/b/c?a=b#d=f');

SELECT '====CUT WWW====';
SELECT cutWWW('http://www.example.com');
SELECT cutWWW('http://www.example.com:1234');
SELECT cutWWW('http://www.example.com/a/b/c');
SELECT cutWWW('http://www.example.com/a/b/c?a=b');
SELECT cutWWW('http://www.example.com/a/b/c?a=b#d=f');
SELECT cutWWW('http://paul@www.example.com/a/b/c?a=b#d=f');
SELECT cutWWW('//paul@www.example.com/a/b/c?a=b#d=f');

SELECT '====CUT QUERY STRING====';
SELECT cutQueryString('http://www.example.com');
SELECT cutQueryString('http://www.example.com:1234');
SELECT cutQueryString('http://www.example.com/a/b/c');
SELECT cutQueryString('http://www.example.com/a/b/c?a=b');
SELECT cutQueryString('http://www.example.com/a/b/c?a=b#d=f');
SELECT cutQueryString('http://paul@www.example.com/a/b/c?a=b#d=f');
SELECT cutQueryString('//paul@www.example.com/a/b/c?a=b#d=f');

SELECT '====CUT FRAGMENT====';
SELECT cutFragment('http://www.example.com');
SELECT cutFragment('http://www.example.com:1234');
SELECT cutFragment('http://www.example.com/a/b/c');
SELECT cutFragment('http://www.example.com/a/b/c?a=b');
SELECT cutFragment('http://www.example.com/a/b/c?a=b#d=f');
SELECT cutFragment('http://paul@www.example.com/a/b/c?a=b#d=f');
SELECT cutFragment('//paul@www.example.com/a/b/c?a=b#d=f');

SELECT '====CUT QUERY STRING AND FRAGMENT====';
SELECT cutQueryStringAndFragment('http://www.example.com');
SELECT cutQueryStringAndFragment('http://www.example.com:1234');
SELECT cutQueryStringAndFragment('http://www.example.com/a/b/c');
SELECT cutQueryStringAndFragment('http://www.example.com/a/b/c?a=b');
SELECT cutQueryStringAndFragment('http://www.example.com/a/b/c?a=b#d=f');
SELECT cutQueryStringAndFragment('http://paul@www.example.com/a/b/c?a=b#d=f');
SELECT cutQueryStringAndFragment('//paul@www.example.com/a/b/c?a=b#d=f');

