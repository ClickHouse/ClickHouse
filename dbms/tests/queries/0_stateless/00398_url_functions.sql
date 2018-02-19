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
