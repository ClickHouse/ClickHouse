SELECT replaceRegexpAll(',,1,,', '^[,]*|[,]*$', '');
SELECT replaceRegexpAll(',,1', '^[,]*|[,]*$', '');
SELECT replaceRegexpAll('1,,', '^[,]*|[,]*$', '');

SELECT replaceRegexpAll(materialize(',,1,,'), '^[,]*|[,]*$', '');
SELECT replaceRegexpAll(materialize(',,1'), '^[,]*|[,]*$', '');
SELECT replaceRegexpAll(materialize('1,,'), '^[,]*|[,]*$', '');

SELECT replaceRegexpAll('a', 'z*', '') == 'a';
SELECT replaceRegexpAll('aa', 'z*', '') == 'aa';
SELECT replaceRegexpAll('aaq', 'z*', '') == 'aaq';
SELECT replaceRegexpAll('aazq', 'z*', '') == 'aaq';
SELECT replaceRegexpAll('aazzq', 'z*', '') == 'aaq';
SELECT replaceRegexpAll('aazzqa', 'z*', '') == 'aaqa';

SELECT replaceRegexpAll(materialize('a'), 'z*', '') == 'a';
SELECT replaceRegexpAll(materialize('aa'), 'z*', '') == 'aa';
SELECT replaceRegexpAll(materialize('aaq'), 'z*', '') == 'aaq';
SELECT replaceRegexpAll(materialize('aazq'), 'z*', '') == 'aaq';
SELECT replaceRegexpAll(materialize('aazzq'), 'z*', '') == 'aaq';
SELECT replaceRegexpAll(materialize('aazzqa'), 'z*', '') == 'aaqa';
