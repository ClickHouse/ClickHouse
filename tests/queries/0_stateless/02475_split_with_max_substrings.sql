select splitByChar(',', '1,2,3');
select splitByChar(',', '1,2,3', -1);
select splitByChar(',', '1,2,3', 0);
select splitByChar(',', '1,2,3', 1);
select splitByChar(',', '1,2,3', 2);
select splitByChar(',', '1,2,3', 3);
select splitByChar(',', '1,2,3', 4);

select splitByRegexp('[ABC]', 'oneAtwoBthreeC');
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', -1);
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', 0);
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', 1);
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', 2);
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', 3);
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', 4);
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', 5);

SELECT alphaTokens('abca1abc');
SELECT alphaTokens('abca1abc', -1);
SELECT alphaTokens('abca1abc', 0);
SELECT alphaTokens('abca1abc', 1);
SELECT alphaTokens('abca1abc', 2);
SELECT alphaTokens('abca1abc', 3);

SELECT splitByAlpha('abca1abc');

SELECT splitByNonAlpha('  1!  a,  b.  ');
SELECT splitByNonAlpha('  1!  a,  b.  ', -1);
SELECT splitByNonAlpha('  1!  a,  b.  ',  0);
SELECT splitByNonAlpha('  1!  a,  b.  ',  1);
SELECT splitByNonAlpha('  1!  a,  b.  ',  2);
SELECT splitByNonAlpha('  1!  a,  b.  ',  3);
SELECT splitByNonAlpha('  1!  a,  b.  ',  4);

SELECT splitByWhitespace('  1!  a,  b.  ');
SELECT splitByWhitespace('  1!  a,  b.  ', -1);
SELECT splitByWhitespace('  1!  a,  b.  ', 0);
SELECT splitByWhitespace('  1!  a,  b.  ', 1);
SELECT splitByWhitespace('  1!  a,  b.  ', 2);
SELECT splitByWhitespace('  1!  a,  b.  ', 3);
SELECT splitByWhitespace('  1!  a,  b.  ', 4);

SELECT splitByString(', ', '1, 2 3, 4,5, abcde');
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', -1);
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', 0);
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', 1);
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', 2);
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', 3);
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', 4);
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', 5);


select splitByChar(',', '1,2,3', ''); -- { serverError 43 }
select splitByRegexp('[ABC]', 'oneAtwoBthreeC', ''); -- { serverError 43 }
SELECT alphaTokens('abca1abc', ''); -- { serverError 43 }
SELECT splitByAlpha('abca1abc', ''); -- { serverError 43 }
SELECT splitByNonAlpha('  1!  a,  b.  ',  ''); -- { serverError 43 }
SELECT splitByWhitespace('  1!  a,  b.  ', ''); -- { serverError 43 }
SELECT splitByString(', ', '1, 2 3, 4,5, abcde', ''); -- { serverError 43 }