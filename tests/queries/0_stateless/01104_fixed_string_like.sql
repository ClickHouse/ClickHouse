SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'hello';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'world';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'xyz';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'hell';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'orld';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%hello%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%world%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%xyz%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%hell%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%orld%';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%hello';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%world';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%xyz';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%hell';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%orld';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'hello%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'world%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'xyz%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'hell%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'orld%';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%he%o%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%w%ld%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%x%z%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%hell_';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '_orld%';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%he__o%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%w__ld%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%x%z%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'hell_';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '_orld';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE 'helloworld';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%helloworld%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%elloworl%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%ow%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%o%w%';

SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%o%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%l%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%l%o%';
SELECT arrayJoin(CAST(['hello', 'world'] AS Array(FixedString(5)))) LIKE '%o%l%';
