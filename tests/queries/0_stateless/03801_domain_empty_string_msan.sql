-- Test for empty and short strings in domain functions to catch use-of-uninitialized-value errors (MSan)

SELECT domainRFC('');
SELECT domainRFC('a');
SELECT domainRFC('/');
SELECT domainRFC('//');

SELECT domainWithoutWWWRFC('');
SELECT domainWithoutWWWRFC('a');
SELECT domainWithoutWWWRFC('/');
SELECT domainWithoutWWWRFC('//');

SELECT domain('');
SELECT domain('a');
SELECT domain('/');
SELECT domain('//');

SELECT domainWithoutWWW('');
SELECT domainWithoutWWW('a');
SELECT domainWithoutWWW('/');
SELECT domainWithoutWWW('//');
