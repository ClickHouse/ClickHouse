SELECT arrayJoin(['hello', 'world']) LIKE 'hello';
SELECT arrayJoin(['hello', 'world']) LIKE 'world';
SELECT arrayJoin(['hello', 'world']) LIKE 'xyz';
SELECT arrayJoin(['hello', 'world']) LIKE 'hell';
SELECT arrayJoin(['hello', 'world']) LIKE 'orld';

SELECT arrayJoin(['hello', 'world']) LIKE '%hello%';
SELECT arrayJoin(['hello', 'world']) LIKE '%world%';
SELECT arrayJoin(['hello', 'world']) LIKE '%xyz%';
SELECT arrayJoin(['hello', 'world']) LIKE '%hell%';
SELECT arrayJoin(['hello', 'world']) LIKE '%orld%';

SELECT arrayJoin(['hello', 'world']) LIKE '%hello';
SELECT arrayJoin(['hello', 'world']) LIKE '%world';
SELECT arrayJoin(['hello', 'world']) LIKE '%xyz';
SELECT arrayJoin(['hello', 'world']) LIKE '%hell';
SELECT arrayJoin(['hello', 'world']) LIKE '%orld';

SELECT arrayJoin(['hello', 'world']) LIKE 'hello%';
SELECT arrayJoin(['hello', 'world']) LIKE 'world%';
SELECT arrayJoin(['hello', 'world']) LIKE 'xyz%';
SELECT arrayJoin(['hello', 'world']) LIKE 'hell%';
SELECT arrayJoin(['hello', 'world']) LIKE 'orld%';

SELECT arrayJoin(['hello', 'world']) LIKE '%he%o%';
SELECT arrayJoin(['hello', 'world']) LIKE '%w%ld%';
SELECT arrayJoin(['hello', 'world']) LIKE '%x%z%';
SELECT arrayJoin(['hello', 'world']) LIKE '%hell_';
SELECT arrayJoin(['hello', 'world']) LIKE '_orld%';

SELECT arrayJoin(['hello', 'world']) LIKE '%he__o%';
SELECT arrayJoin(['hello', 'world']) LIKE '%w__ld%';
SELECT arrayJoin(['hello', 'world']) LIKE '%x%z%';
SELECT arrayJoin(['hello', 'world']) LIKE 'hell_';
SELECT arrayJoin(['hello', 'world']) LIKE '_orld';

SELECT arrayJoin(['hello', 'world']) LIKE 'helloworld';
SELECT arrayJoin(['hello', 'world']) LIKE '%helloworld%';
SELECT arrayJoin(['hello', 'world']) LIKE '%elloworl%';
SELECT arrayJoin(['hello', 'world']) LIKE '%ow%';
SELECT arrayJoin(['hello', 'world']) LIKE '%o%w%';

SELECT arrayJoin(['hello', 'world']) LIKE '%o%';
SELECT arrayJoin(['hello', 'world']) LIKE '%l%';
SELECT arrayJoin(['hello', 'world']) LIKE '%l%o%';
SELECT arrayJoin(['hello', 'world']) LIKE '%o%l%';
