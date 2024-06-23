SELECT 'aяb' LIKE 'a_b';
SELECT 'a\0b' LIKE 'a_b';
SELECT 'a\0b' LIKE 'a\0b';
SELECT 'a\0b' LIKE 'a%\0b';
SELECT 'a\xFFb' LIKE 'a%\xFFb';
SELECT 'a\xFFb' LIKE 'a%\xFF\xFEb';
SELECT 'a\xFFb' LIKE '%a\xFF\xFEb';
SELECT 'a\xFF\xFEb' LIKE '%a\xFF\xFEb';

SELECT materialize('aяb') LIKE 'a_b';
SELECT materialize('a\0b') LIKE 'a_b';
SELECT materialize('a\0b') LIKE 'a\0b';
SELECT materialize('a\0b') LIKE 'a%\0b';
SELECT materialize('a\xFFb') LIKE 'a%\xFFb';
SELECT materialize('a\xFFb') LIKE 'a%\xFF\xFEb';
SELECT materialize('a\xFFb') LIKE '%a\xFF\xFEb';
SELECT materialize('a\xFF\xFEb') LIKE '%a\xFF\xFEb';

SELECT materialize('aяb') LIKE materialize('a_b');
SELECT materialize('a\0b') LIKE materialize('a_b');
SELECT materialize('a\0b') LIKE materialize('a\0b');
SELECT materialize('a\0b') LIKE materialize('a%\0b');
SELECT materialize('a\xFFb') LIKE materialize('a%\xFFb');
SELECT materialize('a\xFFb') LIKE materialize('a%\xFF\xFEb');
SELECT materialize('a\xFFb') LIKE materialize('%a\xFF\xFEb');
SELECT materialize('a\xFF\xFEb') LIKE materialize('%a\xFF\xFEb');
