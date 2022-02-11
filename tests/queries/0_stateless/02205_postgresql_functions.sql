--- REGEXP_MATCHES
select match('a key="v" ', 'key="(.*?)"'), REGEXP_MATCHES('a key="v" ', 'key="(.*?)"');
select match(materialize('a key="v" '), 'key="(.*?)"'), REGEXP_MATCHES(materialize('a key="v" '), 'key="(.*?)"');

select match('\0 key="v" ', 'key="(.*?)"'), REGEXP_MATCHES('\0 key="v" ', 'key="(.*?)"');
select match(materialize('\0 key="v" '), 'key="(.*?)"'), REGEXP_MATCHES(materialize('\0 key="v" '), 'key="(.*?)"');


--- REGEXP_REPLACE
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '_._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['._', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['._', '._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['._', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['._', '_._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_.', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_.', '._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_.', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_.', '_._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_._', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_._', '._']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_._', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['_._', '_._']) AS s);

SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['.', '.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['.', '._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['.', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['.', '_._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['._', '.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['._', '._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['._', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['._', '_._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_.', '.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_.', '._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_.', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_.', '_._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_._', '.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_._', '._']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_._', '_.']) AS s);
SELECT s, replaceAll(s, '_', 'oo') AS a, REGEXP_REPLACE(s, '_', 'oo') AS b, a = b FROM (SELECT arrayJoin(['_._', '_._']) AS s);

SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '__.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.', '__.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.__', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.__', '.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.__', '__.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['.__', '__.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.', '.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.', '__.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.', '__.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.__', '.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.__', '.__']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.__', '__.']) AS s);
SELECT s, replaceAll(s, '_', 'o') AS a, REGEXP_REPLACE(s, '_', 'o') AS b, a = b FROM (SELECT arrayJoin(['__.__', '__.__']) AS s);
