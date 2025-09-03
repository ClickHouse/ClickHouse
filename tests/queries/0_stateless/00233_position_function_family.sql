-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

SET send_logs_level = 'fatal';

select 1 = position('', '');
select 1 = position('abc', '');
select 0 = position('', 'abc');
select 1 = position('abc', 'abc');
select 2 = position('abc', 'bc');
select 3 = position('abc', 'c');

select 1 = position('', '', 0);
select 1 = position('', '', 1);
select 0 = position('', '', 2);
select 1 = position('a', '', 1);
select 2 = position('a', '', 2);
select 0 = position('a', '', 3);

select [1, 1, 2, 3, 4, 5, 0, 0, 0, 0] = groupArray(position('aaaa', '', number)) from numbers(10);
select [1, 1, 2, 3, 4, 5, 0, 0, 0, 0] = groupArray(position(materialize('aaaa'), '', number)) from numbers(10);
select [1, 1, 2, 3, 4, 5, 0, 0, 0, 0] = groupArray(position('aaaa', materialize(''), number)) from numbers(10);
select [1, 1, 2, 3, 4, 5, 0, 0, 0, 0] = groupArray(position(materialize('aaaa'), materialize(''), number)) from numbers(10);

select [1, 1, 2, 3, 4, 0, 0, 0, 0, 0] = groupArray(position('aaaa', 'a', number)) from numbers(10);
select [1, 1, 2, 3, 4, 0, 0, 0, 0, 0] = groupArray(position(materialize('aaaa'), 'a', number)) from numbers(10);
select [1, 1, 2, 3, 4, 0, 0, 0, 0, 0] = groupArray(position('aaaa', materialize('a'), number)) from numbers(10);
select [1, 1, 2, 3, 4, 0, 0, 0, 0, 0] = groupArray(position(materialize('aaaa'), materialize('a'), number)) from numbers(10);

select 1 = position(materialize(''), '');
select 1 = position(materialize('abc'), '');
select 0 = position(materialize(''), 'abc');
select 1 = position(materialize('abc'), 'abc');
select 2 = position(materialize('abc'), 'bc');
select 3 = position(materialize('abc'), 'c');

select 1 = position(materialize(''), '') from system.numbers limit 10;
select 1 = position(materialize('abc'), '') from system.numbers limit 10;
select 0 = position(materialize(''), 'abc') from system.numbers limit 10;
select 1 = position(materialize('abc'), 'abc') from system.numbers limit 10;
select 2 = position(materialize('abc'), 'bc') from system.numbers limit 10;
select 3 = position(materialize('abc'), 'c') from system.numbers limit 10;

select 1 = position('', '');
select 1 = position('абв', '');
select 0 = position('', 'абв');
select 1 = position('абв', 'абв');
select 3 = position('абв', 'бв');
select 5 = position('абв', 'в');

select 2 = position('abcabc', 'b', 0);
select 2 = position('abcabc', 'b', 1);
select 2 = position('abcabc', 'b', 2);
select 5 = position('abcabc', 'b', 3);
select 5 = position('abcabc', 'b', 4);
select 5 = position('abcabc', 'b', 5);
select 0 = position('abcabc', 'b', 6);
select 2 = position('abcabc', 'bca', 0);
select 0 = position('abcabc', 'bca', 3);

select 1 = position(materialize(''), '');
select 1 = position(materialize('абв'), '');
select 0 = position(materialize(''), 'абв');
select 1 = position(materialize('абв'), 'абв');
select 3 = position(materialize('абв'), 'бв');
select 5 = position(materialize('абв'), 'в');

select 1 = position(materialize(''), '') from system.numbers limit 10;
select 1 = position(materialize('абв'), '') from system.numbers limit 10;
select 0 = position(materialize(''), 'абв') from system.numbers limit 10;
select 1 = position(materialize('абв'), 'абв') from system.numbers limit 10;
select 3 = position(materialize('абв'), 'бв') from system.numbers limit 10;
select 5 = position(materialize('абв'), 'в') from system.numbers limit 10;

select 1 = positionUTF8('', '');
select 1 = positionUTF8('абв', '');
select 0 = positionUTF8('', 'абв');
select 1 = positionUTF8('абв', 'абв');
select 2 = positionUTF8('абв', 'бв');
select 3 = positionUTF8('абв', 'в');

select 3 = position('абвабв', 'б', 2);
select 3 = position('абвабв', 'б', 3);
select 3 = position('абвабв', 'бва', 2);
select 9 = position('абвабв', 'б', 4);
select 0 = position('абвабв', 'бва', 4);
select 5 = position('абвабв', 'в', 0);
select 11 = position('абвабв', 'в', 6);

select 1 = positionUTF8(materialize(''), '');
select 1 = positionUTF8(materialize('абв'), '');
select 0 = positionUTF8(materialize(''), 'абв');
select 1 = positionUTF8(materialize('абв'), 'абв');
select 2 = positionUTF8(materialize('абв'), 'бв');
select 3 = positionUTF8(materialize('абв'), 'в');

select 1 = positionUTF8(materialize(''), '') from system.numbers limit 10;
select 1 = positionUTF8(materialize('абв'), '') from system.numbers limit 10;
select 0 = positionUTF8(materialize(''), 'абв') from system.numbers limit 10;
select 1 = positionUTF8(materialize('абв'), 'абв') from system.numbers limit 10;
select 2 = positionUTF8(materialize('абв'), 'бв') from system.numbers limit 10;
select 3 = positionUTF8(materialize('абв'), 'в') from system.numbers limit 10;

select 2 = positionUTF8('абвабв', 'б', 0);
select 2 = positionUTF8('абвабв', 'б', 1);
select 2 = positionUTF8('абвабв', 'б', 2);
select 5 = positionUTF8('абвабв', 'б', 3);
select 5 = positionUTF8('абвабв', 'б', 4);
select 5 = positionUTF8('абвабв', 'б', 5);
select 0 = positionUTF8('абвабв', 'б', 6);
select 2 = positionUTF8('абвабв', 'бва', 0);
select 0 = positionUTF8('абвабв', 'бва', 3);

select 2 = positionUTF8(materialize('абвабв'), 'б', 0) from system.numbers limit 10;
select 2 = positionUTF8(materialize('абвабв'), 'б', 1) from system.numbers limit 10;
select 2 = positionUTF8(materialize('абвабв'), 'б', 2) from system.numbers limit 10;
select 5 = positionUTF8(materialize('абвабв'), 'б', 3) from system.numbers limit 10;
select 5 = positionUTF8(materialize('абвабв'), 'б', 4) from system.numbers limit 10;
select 5 = positionUTF8(materialize('абвабв'), 'б', 5) from system.numbers limit 10;
select 0 = positionUTF8(materialize('абвабв'), 'б', 6) from system.numbers limit 10;
select 2 = positionUTF8(materialize('абвабв'), 'бва', 0) from system.numbers limit 10;
select 0 = positionUTF8(materialize('абвабв'), 'бва', 3) from system.numbers limit 10;

select 2 = positionUTF8('абвабв', materialize('б'), 0) from system.numbers limit 10;
select 2 = positionUTF8('абвабв', materialize('б'), 1) from system.numbers limit 10;
select 2 = positionUTF8('абвабв', materialize('б'), 2) from system.numbers limit 10;
select 5 = positionUTF8('абвабв', materialize('б'), 3) from system.numbers limit 10;
select 5 = positionUTF8('абвабв', materialize('б'), 4) from system.numbers limit 10;
select 5 = positionUTF8('абвабв', materialize('б'), 5) from system.numbers limit 10;
select 0 = positionUTF8('абвабв', materialize('б'), 6) from system.numbers limit 10;
select 2 = positionUTF8('абвабв', materialize('бва'), 0) from system.numbers limit 10;
select 0 = positionUTF8('абвабв', materialize('бва'), 3) from system.numbers limit 10;

select 2 = positionUTF8(materialize('абвабв'), materialize('б'), 0) from system.numbers limit 10;
select 2 = positionUTF8(materialize('абвабв'), materialize('б'), 1) from system.numbers limit 10;
select 2 = positionUTF8(materialize('абвабв'), materialize('б'), 2) from system.numbers limit 10;
select 5 = positionUTF8(materialize('абвабв'), materialize('б'), 3) from system.numbers limit 10;
select 5 = positionUTF8(materialize('абвабв'), materialize('б'), 4) from system.numbers limit 10;
select 5 = positionUTF8(materialize('абвабв'), materialize('б'), 5) from system.numbers limit 10;
select 0 = positionUTF8(materialize('абвабв'), materialize('б'), 6) from system.numbers limit 10;
select 2 = positionUTF8(materialize('абвабв'), materialize('бва'), 0) from system.numbers limit 10;
select 0 = positionUTF8(materialize('абвабв'), materialize('бва'), 3) from system.numbers limit 10;

select [2, 2, 2, 5, 5, 5, 0, 0, 0, 0] = groupArray(positionUTF8(materialize('абвабв'), materialize('б'), number)) from numbers(10);
select [2, 2, 2, 5, 5, 5, 0, 0, 0, 0] = groupArray(positionUTF8('абвабв', materialize('б'), number)) from numbers(10);
select [2, 2, 2, 5, 5, 5, 0, 0, 0, 0] = groupArray(positionUTF8('абвабв', 'б', number)) from numbers(10);
select [2, 2, 2, 5, 5, 5, 0, 0, 0, 0] = groupArray(positionUTF8(materialize('абвабв'), 'б', number)) from numbers(10);

select 1 = positionCaseInsensitive('', '');
select 1 = positionCaseInsensitive('abc', '');
select 0 = positionCaseInsensitive('', 'aBc');
select 1 = positionCaseInsensitive('abc', 'aBc');
select 2 = positionCaseInsensitive('abc', 'Bc');
select 3 = positionCaseInsensitive('abc', 'C');

select 1 = positionCaseInsensitive(materialize(''), '');
select 1 = positionCaseInsensitive(materialize('abc'), '');
select 0 = positionCaseInsensitive(materialize(''), 'aBc');
select 1 = positionCaseInsensitive(materialize('abc'), 'aBc');
select 2 = positionCaseInsensitive(materialize('abc'), 'Bc');
select 3 = positionCaseInsensitive(materialize('abc'), 'C');

select 1 = positionCaseInsensitive(materialize(''), '') from system.numbers limit 10;
select 1 = positionCaseInsensitive(materialize('abc'), '') from system.numbers limit 10;
select 0 = positionCaseInsensitive(materialize(''), 'aBc') from system.numbers limit 10;
select 1 = positionCaseInsensitive(materialize('abc'), 'aBc') from system.numbers limit 10;
select 2 = positionCaseInsensitive(materialize('abc'), 'Bc') from system.numbers limit 10;
select 3 = positionCaseInsensitive(materialize('abc'), 'C') from system.numbers limit 10;

select 6 = positionCaseInsensitive(materialize('abcabc'), 'C', 4);
select 6 = positionCaseInsensitive(materialize('abcabc'), 'C', 4) from system.numbers limit 10;
select 6 = positionCaseInsensitive(materialize('abcabc'), 'C', materialize(4)) from system.numbers limit 10;

select 1 = positionCaseInsensitive('', '');
select 1 = positionCaseInsensitive('абв', '');
select 0 = positionCaseInsensitive('', 'аБв');
select 0 = positionCaseInsensitive('абв', 'аБв');
select 0 = positionCaseInsensitive('абв', 'Бв');
select 0 = positionCaseInsensitive('абв', 'В');

select 1 = positionCaseInsensitive(materialize(''), '');
select 1 = positionCaseInsensitive(materialize('абв'), '');
select 0 = positionCaseInsensitive(materialize(''), 'аБв');
select 0 = positionCaseInsensitive(materialize('абв'), 'аБв');
select 0 = positionCaseInsensitive(materialize('абв'), 'Бв');
select 0 = positionCaseInsensitive(materialize('абв'), 'В');

select 1 = positionCaseInsensitive(materialize(''), '') from system.numbers limit 10;
select 1 = positionCaseInsensitive(materialize('абв'), '') from system.numbers limit 10;
select 0 = positionCaseInsensitive(materialize(''), 'аБв') from system.numbers limit 10;
select 0 = positionCaseInsensitive(materialize('абв'), 'аБв') from system.numbers limit 10;
select 0 = positionCaseInsensitive(materialize('абв'), 'Бв') from system.numbers limit 10;
select 0 = positionCaseInsensitive(materialize('абв'), 'В') from system.numbers limit 10;

select 1 = positionCaseInsensitiveUTF8('', '');
select 1 = positionCaseInsensitiveUTF8('абв', '');
select 0 = positionCaseInsensitiveUTF8('', 'аБв');
select 1 = positionCaseInsensitiveUTF8('абв', 'аБв');
select 2 = positionCaseInsensitiveUTF8('абв', 'Бв');
select 3 = positionCaseInsensitiveUTF8('абв', 'в');

select 1 = positionCaseInsensitiveUTF8(materialize(''), '');
select 1 = positionCaseInsensitiveUTF8(materialize('абв'), '');
select 0 = positionCaseInsensitiveUTF8(materialize(''), 'аБв');
select 1 = positionCaseInsensitiveUTF8(materialize('абв'), 'аБв');
select 2 = positionCaseInsensitiveUTF8(materialize('абв'), 'Бв');
select 3 = positionCaseInsensitiveUTF8(materialize('абв'), 'В');

select 1 = positionCaseInsensitiveUTF8(materialize(''), '') from system.numbers limit 10;
select 1 = positionCaseInsensitiveUTF8(materialize('абв'), '') from system.numbers limit 10;
select 0 = positionCaseInsensitiveUTF8(materialize(''), 'аБв') from system.numbers limit 10;
select 1 = positionCaseInsensitiveUTF8(materialize('абв'), 'аБв') from system.numbers limit 10;
select 2 = positionCaseInsensitiveUTF8(materialize('абв'), 'Бв') from system.numbers limit 10;
select 3 = positionCaseInsensitiveUTF8(materialize('абв'), 'В') from system.numbers limit 10;

select 6 = positionCaseInsensitiveUTF8(materialize('абвабв'), 'В', 4);
select 6 = positionCaseInsensitiveUTF8(materialize('абвабв'), 'В', 4) from system.numbers limit 10;
select 6 = positionCaseInsensitiveUTF8(materialize('абвабв'), 'В', materialize(4)) from system.numbers limit 10;

select position('' as h, '' as n) = positionCaseInsensitive(h, n);
select position('abc' as h, '' as n) = positionCaseInsensitive(n, n);
select 0 = positionCaseInsensitive('', 'aBc');
select position('abc' as h, lower('aBc' as n)) = positionCaseInsensitive(h, n);
select position('abc' as h, lower('Bc' as n)) = positionCaseInsensitive(h, n);
select position('abc' as h, lower('C' as n)) = positionCaseInsensitive(h, n);

select positionCaseInsensitive(materialize('') as h, '' as n) = positionCaseInsensitive(h, n);
select positionCaseInsensitive(materialize('abc') as h, '' as n) = positionCaseInsensitive(h, n);
select positionCaseInsensitive(materialize('') as h, lower('aBc' as n)) = positionCaseInsensitive(h, n);
select positionCaseInsensitive(materialize('abc') as h, lower('aBc' as n)) = positionCaseInsensitive(h, n);
select positionCaseInsensitive(materialize('abc') as h, lower('Bc' as n)) = positionCaseInsensitive(h, n);
select positionCaseInsensitive(materialize('abc') as h, lower('C' as n)) = positionCaseInsensitive(h, n);

select position(materialize('') as h, lower('' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('abc') as h, lower('' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('') as h, lower('aBc' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('abc') as h, lower('aBc' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('abc') as h, lower('Bc' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('abc') as h, lower('C' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;

select position('' as h, lower('' as n)) = positionCaseInsensitive(h, n);
select position('абв' as h, lower('' as n)) = positionCaseInsensitive(h, n);
select position('' as h, lower('аБв' as n)) = positionCaseInsensitive(h, n);
select position('абв' as h, lower('аБв' as n)) = positionCaseInsensitive(h, n);
select position('абв' as h, lower('Бв' as n)) = positionCaseInsensitive(h, n);
select position('абв' as h, lower('В' as n)) = positionCaseInsensitive(h, n);

select position(materialize('') as h, lower('' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('' as n)) = positionCaseInsensitive(h, n);
select position(materialize('') as h, lower('аБв' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('аБв' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('Бв' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('В' as n)) = positionCaseInsensitive(h, n);

select position(materialize('') as h, lower('' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('' as n)) = positionCaseInsensitive(h, n);
select position(materialize('') as h, lower('аБв' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('аБв' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('Бв' as n)) = positionCaseInsensitive(h, n);
select position(materialize('абв') as h, lower('В' as n)) = positionCaseInsensitive(h, n);

select position(materialize('') as h, lower('' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('абв') as h, lower('' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('') as h, lower('аБв' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('абв') as h, lower('аБв' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('абв') as h, lower('Бв' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;
select position(materialize('абв') as h, lower('В' as n)) = positionCaseInsensitive(h, n) from system.numbers limit 10;

select positionUTF8('' as h, lowerUTF8('' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8('абв' as h, lowerUTF8('' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8('' as h, lowerUTF8('аБв' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8('абв' as h, lowerUTF8('аБв' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8('абв' as h, lowerUTF8('Бв' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8('абв' as h, lowerUTF8('в' as n)) = positionCaseInsensitiveUTF8(h, n);

select positionUTF8(materialize('') as h, lowerUTF8('' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8(materialize('абв') as h, lowerUTF8('' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8(materialize('') as h, lowerUTF8('аБв' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8(materialize('абв') as h, lowerUTF8('аБв' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8(materialize('абв') as h, lowerUTF8('Бв' as n)) = positionCaseInsensitiveUTF8(h, n);
select positionUTF8(materialize('абв') as h, lowerUTF8('В' as n)) = positionCaseInsensitiveUTF8(h, n);

select positionUTF8(materialize('') as h, lowerUTF8('' as n)) = positionCaseInsensitiveUTF8(h, n) from system.numbers limit 10;
select positionUTF8(materialize('абв') as h, lowerUTF8('' as n)) = positionCaseInsensitiveUTF8(h, n) from system.numbers limit 10;
select positionUTF8(materialize('') as h, lowerUTF8('аБв' as n)) = positionCaseInsensitiveUTF8(h, n) from system.numbers limit 10;
select positionUTF8(materialize('абв') as h, lowerUTF8('аБв' as n)) = positionCaseInsensitiveUTF8(h, n) from system.numbers limit 10;
select positionUTF8(materialize('абв') as h, lowerUTF8('Бв' as n)) = positionCaseInsensitiveUTF8(h, n) from system.numbers limit 10;
select positionUTF8(materialize('абв') as h, lowerUTF8('В' as n)) = positionCaseInsensitiveUTF8(h, n) from system.numbers limit 10;


select 2 = position('abcdefgh', materialize('b'));
select 2 = position('abcdefgh', materialize('bc'));
select 2 = position('abcdefgh', materialize('bcd'));
select 2 = position('abcdefgh', materialize('bcde'));
select 2 = position('abcdefgh', materialize('bcdef'));
select 2 = position('abcdefgh', materialize('bcdefg'));
select 2 = position('abcdefgh', materialize('bcdefgh'));

select 1 = position('abcdefgh', materialize('abcdefgh'));
select 1 = position('abcdefgh', materialize('abcdefg'));
select 1 = position('abcdefgh', materialize('abcdef'));
select 1 = position('abcdefgh', materialize('abcde'));
select 1 = position('abcdefgh', materialize('abcd'));
select 1 = position('abcdefgh', materialize('abc'));
select 1 = position('abcdefgh', materialize('ab'));
select 1 = position('abcdefgh', materialize('a'));

select 3 = position('abcdefgh', materialize('c'));
select 3 = position('abcdefgh', materialize('cd'));
select 3 = position('abcdefgh', materialize('cde'));
select 3 = position('abcdefgh', materialize('cdef'));
select 3 = position('abcdefgh', materialize('cdefg'));
select 3 = position('abcdefgh', materialize('cdefgh'));

select 4 = position('abcdefgh', materialize('defgh'));
select 4 = position('abcdefgh', materialize('defg'));
select 4 = position('abcdefgh', materialize('def'));
select 4 = position('abcdefgh', materialize('de'));
select 4 = position('abcdefgh', materialize('d'));

select 5 = position('abcdefgh', materialize('e'));
select 5 = position('abcdefgh', materialize('ef'));
select 5 = position('abcdefgh', materialize('efg'));
select 5 = position('abcdefgh', materialize('efgh'));

select 6 = position('abcdefgh', materialize('fgh'));
select 6 = position('abcdefgh', materialize('fg'));
select 6 = position('abcdefgh', materialize('f'));

select 7 = position('abcdefgh', materialize('g'));
select 7 = position('abcdefgh', materialize('gh'));

select 8 = position('abcdefgh', materialize('h'));

select 2 = position('abcdefgh', materialize('b')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bc')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bcd')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bcde')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bcdef')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bcdefg')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bcdefgh')) from system.numbers limit 10;

select 1 = position('abcdefgh', materialize('abcdefgh')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('abcdefg')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('abcdef')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('abcde')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('abcd')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('abc')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('ab')) from system.numbers limit 10;
select 1 = position('abcdefgh', materialize('a')) from system.numbers limit 10;

select 3 = position('abcdefgh', materialize('c')) from system.numbers limit 10;
select 3 = position('abcdefgh', materialize('cd')) from system.numbers limit 10;
select 3 = position('abcdefgh', materialize('cde')) from system.numbers limit 10;
select 3 = position('abcdefgh', materialize('cdef')) from system.numbers limit 10;
select 3 = position('abcdefgh', materialize('cdefg')) from system.numbers limit 10;
select 3 = position('abcdefgh', materialize('cdefgh')) from system.numbers limit 10;

select 4 = position('abcdefgh', materialize('defgh')) from system.numbers limit 10;
select 4 = position('abcdefgh', materialize('defg')) from system.numbers limit 10;
select 4 = position('abcdefgh', materialize('def')) from system.numbers limit 10;
select 4 = position('abcdefgh', materialize('de')) from system.numbers limit 10;
select 4 = position('abcdefgh', materialize('d')) from system.numbers limit 10;

select 5 = position('abcdefgh', materialize('e')) from system.numbers limit 10;
select 5 = position('abcdefgh', materialize('ef')) from system.numbers limit 10;
select 5 = position('abcdefgh', materialize('efg')) from system.numbers limit 10;
select 5 = position('abcdefgh', materialize('efgh')) from system.numbers limit 10;

select 6 = position('abcdefgh', materialize('fgh')) from system.numbers limit 10;
select 6 = position('abcdefgh', materialize('fg')) from system.numbers limit 10;
select 6 = position('abcdefgh', materialize('f')) from system.numbers limit 10;

select 7 = position('abcdefgh', materialize('g')) from system.numbers limit 10;
select 7 = position('abcdefgh', materialize('gh')) from system.numbers limit 10;

select 8 = position('abcdefgh', materialize('h')) from system.numbers limit 10;

select 2 = position('abcdefgh', materialize('b')) from system.numbers limit 129;
select 2 = position('abcdefgh', materialize('bc')) from system.numbers limit 129;
select 2 = position('abcdefgh', materialize('bcd')) from system.numbers limit 10;
select 2 = position('abcdefgh', materialize('bcde')) from system.numbers limit 129;
select 2 = position('abcdefgh', materialize('bcdef')) from system.numbers limit 129;
select 2 = position('abcdefgh', materialize('bcdefg')) from system.numbers limit 129;
select 2 = position('abcdefgh', materialize('bcdefgh')) from system.numbers limit 129;

select 1 = position('abcdefgh', materialize('abcdefgh')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('abcdefg')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('abcdef')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('abcde')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('abcd')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('abc')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('ab')) from system.numbers limit 129;
select 1 = position('abcdefgh', materialize('a')) from system.numbers limit 129;

select 3 = position('abcdefgh', materialize('c')) from system.numbers limit 129;
select 3 = position('abcdefgh', materialize('cd')) from system.numbers limit 129;
select 3 = position('abcdefgh', materialize('cde')) from system.numbers limit 129;
select 3 = position('abcdefgh', materialize('cdef')) from system.numbers limit 129;
select 3 = position('abcdefgh', materialize('cdefg')) from system.numbers limit 129;
select 3 = position('abcdefgh', materialize('cdefgh')) from system.numbers limit 129;

select 4 = position('abcdefgh', materialize('defgh')) from system.numbers limit 129;
select 4 = position('abcdefgh', materialize('defg')) from system.numbers limit 129;
select 4 = position('abcdefgh', materialize('def')) from system.numbers limit 129;
select 4 = position('abcdefgh', materialize('de')) from system.numbers limit 129;
select 4 = position('abcdefgh', materialize('d')) from system.numbers limit 129;

select 5 = position('abcdefgh', materialize('e')) from system.numbers limit 129;
select 5 = position('abcdefgh', materialize('ef')) from system.numbers limit 129;
select 5 = position('abcdefgh', materialize('efg')) from system.numbers limit 129;
select 5 = position('abcdefgh', materialize('efgh')) from system.numbers limit 129;

select 6 = position('abcdefgh', materialize('fgh')) from system.numbers limit 129;
select 6 = position('abcdefgh', materialize('fg')) from system.numbers limit 129;
select 6 = position('abcdefgh', materialize('f')) from system.numbers limit 129;

select 7 = position('abcdefgh', materialize('g')) from system.numbers limit 129;
select 7 = position('abcdefgh', materialize('gh')) from system.numbers limit 129;

select 8 = position('abcdefgh', materialize('h')) from system.numbers limit 129;

select 2 = position('abc', materialize('b'));
select 2 = position('abc', materialize('bc'));
select 0 = position('abc', materialize('bcde'));
select 0 = position('abc', materialize('bcdef'));
select 0 = position('abc', materialize('bcdefg'));
select 0 = position('abc', materialize('bcdefgh'));

select 0 = position('abc', materialize('abcdefg'));
select 0 = position('abc', materialize('abcdef'));
select 0 = position('abc', materialize('abcde'));
select 0 = position('abc', materialize('abcd'));
select 1 = position('abc', materialize('abc'));
select 1 = position('abc', materialize('ab'));
select 1 = position('abc', materialize('a'));

select 3 = position('abcd', materialize('c'));
select 3 = position('abcd', materialize('cd'));
select 0 = position('abcd', materialize('cde'));
select 0 = position('abcd', materialize('cdef'));
select 0 = position('abcd', materialize('cdefg'));
select 0 = position('abcd', materialize('cdefgh'));

select 0 = position('abc', materialize('defgh'));
select 0 = position('abc', materialize('defg'));
select 0 = position('abc', materialize('def'));
select 0 = position('abc', materialize('de'));
select 0 = position('abc', materialize('d'));


select 2 = position('abc', materialize('b')) from system.numbers limit 10;
select 2 = position('abc', materialize('bc')) from system.numbers limit 10;
select 0 = position('abc', materialize('bcde')) from system.numbers limit 10;
select 0 = position('abc', materialize('bcdef')) from system.numbers limit 10;
select 0 = position('abc', materialize('bcdefg')) from system.numbers limit 10;
select 0 = position('abc', materialize('bcdefgh')) from system.numbers limit 10;


select 0 = position('abc', materialize('abcdefg')) from system.numbers limit 10;
select 0 = position('abc', materialize('abcdef')) from system.numbers limit 10;
select 0 = position('abc', materialize('abcde')) from system.numbers limit 10;
select 0 = position('abc', materialize('abcd')) from system.numbers limit 10;
select 1 = position('abc', materialize('abc')) from system.numbers limit 10;
select 1 = position('abc', materialize('ab')) from system.numbers limit 10;
select 1 = position('abc', materialize('a')) from system.numbers limit 10;

select 3 = position('abcd', materialize('c')) from system.numbers limit 10;
select 3 = position('abcd', materialize('cd')) from system.numbers limit 10;
select 0 = position('abcd', materialize('cde')) from system.numbers limit 10;
select 0 = position('abcd', materialize('cdef')) from system.numbers limit 10;
select 0 = position('abcd', materialize('cdefg')) from system.numbers limit 10;
select 0 = position('abcd', materialize('cdefgh')) from system.numbers limit 10;

select 0 = position('abc', materialize('defgh')) from system.numbers limit 10;
select 0 = position('abc', materialize('defg')) from system.numbers limit 10;
select 0 = position('abc', materialize('def')) from system.numbers limit 10;
select 0 = position('abc', materialize('de')) from system.numbers limit 10;
select 0 = position('abc', materialize('d')) from system.numbers limit 10;

select 1 = position('abc', materialize(''));
select 1 = position('abc', materialize('')) from system.numbers limit 10;
select 1 = position('abc', materialize('')) from system.numbers limit 100;
select 1 = position('abc', materialize('')) from system.numbers limit 1000;

select 1 = position('abab', materialize('ab'));
select 1 = position('abababababababababababab', materialize('abab'));
select 1 = position('abababababababababababab', materialize('abababababababababa'));

select positionUTF8('你', '', 3) = positionUTF8(materialize('你'), materialize(''), materialize(3));
