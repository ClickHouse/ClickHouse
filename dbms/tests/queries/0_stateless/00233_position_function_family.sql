SET send_logs_level = 'none';
select 1 = position('', '');
select 1 = position('abc', '');
select 0 = position('', 'abc');
select 1 = position('abc', 'abc');
select 2 = position('abc', 'bc');
select 3 = position('abc', 'c');

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


select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['b']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bc']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcd']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcde']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdef']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefg']);
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefgh']);

select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefgh']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefg']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdef']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcde']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcd']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abc']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['ab']);
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['a']);

select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['c']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cd']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cde']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdef']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefg']);
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefgh']);

select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defgh']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defg']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['def']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['de']);
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['d']);

select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['e']);
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['ef']);
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efg']);
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efgh']);

select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fgh']);
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fg']);
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['f']);

select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['g']);
select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['gh']);

select [8] = multiSearchAllPositions(materialize('abcdefgh'), ['h']);

select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['b']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bc']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcde']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 10;

select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcde']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcd']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abc']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['ab']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['a']) from system.numbers limit 10;

select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['c']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cd']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cde']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdef']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 10;

select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defgh']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defg']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['def']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['de']) from system.numbers limit 10;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['d']) from system.numbers limit 10;

select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['e']) from system.numbers limit 10;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['ef']) from system.numbers limit 10;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efg']) from system.numbers limit 10;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efgh']) from system.numbers limit 10;

select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fgh']) from system.numbers limit 10;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fg']) from system.numbers limit 10;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['f']) from system.numbers limit 10;

select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['g']) from system.numbers limit 10;
select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['gh']) from system.numbers limit 10;

select [8] = multiSearchAllPositions(materialize('abcdefgh'), ['h']) from system.numbers limit 10;

select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['b']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bc']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcde']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 129;
select [2] = multiSearchAllPositions(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 129;

select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcde']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abcd']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['abc']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['ab']) from system.numbers limit 129;
select [1] = multiSearchAllPositions(materialize('abcdefgh'), ['a']) from system.numbers limit 129;

select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['c']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cd']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cde']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdef']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 129;
select [3] = multiSearchAllPositions(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 129;

select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defgh']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['defg']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['def']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['de']) from system.numbers limit 129;
select [4] = multiSearchAllPositions(materialize('abcdefgh'), ['d']) from system.numbers limit 129;

select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['e']) from system.numbers limit 129;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['ef']) from system.numbers limit 129;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efg']) from system.numbers limit 129;
select [5] = multiSearchAllPositions(materialize('abcdefgh'), ['efgh']) from system.numbers limit 129;

select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fgh']) from system.numbers limit 129;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['fg']) from system.numbers limit 129;
select [6] = multiSearchAllPositions(materialize('abcdefgh'), ['f']) from system.numbers limit 129;

select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['g']) from system.numbers limit 129;
select [7] = multiSearchAllPositions(materialize('abcdefgh'), ['gh']) from system.numbers limit 129;

select [8] = multiSearchAllPositions(materialize('abcdefgh'), ['h']) from system.numbers limit 129;

select [2] = multiSearchAllPositions(materialize('abc'), ['b']);
select [2] = multiSearchAllPositions(materialize('abc'), ['bc']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcde']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdef']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefg']);
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefgh']);

select [0] = multiSearchAllPositions(materialize('abc'), ['abcdefg']);
select [0] = multiSearchAllPositions(materialize('abc'), ['abcdef']);
select [0] = multiSearchAllPositions(materialize('abc'), ['abcde']);
select [0] = multiSearchAllPositions(materialize('abc'), ['abcd']);
select [1] = multiSearchAllPositions(materialize('abc'), ['abc']);
select [1] = multiSearchAllPositions(materialize('abc'), ['ab']);
select [1] = multiSearchAllPositions(materialize('abc'), ['a']);

select [3] = multiSearchAllPositions(materialize('abcd'), ['c']);
select [3] = multiSearchAllPositions(materialize('abcd'), ['cd']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cde']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdef']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefg']);
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefgh']);

select [0] = multiSearchAllPositions(materialize('abc'), ['defgh']);
select [0] = multiSearchAllPositions(materialize('abc'), ['defg']);
select [0] = multiSearchAllPositions(materialize('abc'), ['def']);
select [0] = multiSearchAllPositions(materialize('abc'), ['de']);
select [0] = multiSearchAllPositions(materialize('abc'), ['d']);


select [2] = multiSearchAllPositions(materialize('abc'), ['b']) from system.numbers limit 10;
select [2] = multiSearchAllPositions(materialize('abc'), ['bc']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcde']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdef']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['bcdefgh']) from system.numbers limit 10;


select [0] = multiSearchAllPositions(materialize('abc'), ['abcdefg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['abcdef']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['abcde']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['abcd']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['abc']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['ab']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['a']) from system.numbers limit 10;

select [3] = multiSearchAllPositions(materialize('abcd'), ['c']) from system.numbers limit 10;
select [3] = multiSearchAllPositions(materialize('abcd'), ['cd']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cde']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdef']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abcd'), ['cdefgh']) from system.numbers limit 10;

select [0] = multiSearchAllPositions(materialize('abc'), ['defgh']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['defg']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['def']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['de']) from system.numbers limit 10;
select [0] = multiSearchAllPositions(materialize('abc'), ['d']) from system.numbers limit 10;

select [1] = multiSearchAllPositions(materialize('abc'), ['']);
select [1] = multiSearchAllPositions(materialize('abc'), ['']) from system.numbers limit 10;
select [1] = multiSearchAllPositions(materialize('abc'), ['']) from system.numbers limit 100;
select [1] = multiSearchAllPositions(materialize('abc'), ['']) from system.numbers limit 1000;

select [1] = multiSearchAllPositions(materialize('abab'), ['ab']);
select [1] = multiSearchAllPositions(materialize('abababababababababababab'), ['abab']);
select [1] = multiSearchAllPositions(materialize('abababababababababababab'), ['abababababababababa']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['b']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bc']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcd']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcde']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefgh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefgh']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcde']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcd']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['abc']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['ab']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['a']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['c']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cd']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cde']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefgh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['defgh']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['defg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['def']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['de']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['d']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['e']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['ef']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['efg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['efgh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['fgh']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['fg']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['f']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['g']);
select 1 = multiSearchAny(materialize('abcdefgh'), ['gh']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['h']);

select 1 = multiSearchAny(materialize('abcdefgh'), ['b']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bc']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcde']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcde']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abc']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ab']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['a']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['c']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cde']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['defgh']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['defg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['def']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['de']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['d']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['e']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ef']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efgh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['fgh']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['fg']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['f']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['g']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['gh']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['h']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcdefgh'), ['b']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bc']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcde']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['bcdefgh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefgh']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdefg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcdef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcde']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abcd']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['abc']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ab']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['a']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['c']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cd']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cde']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['cdefgh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['defgh']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['defg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['def']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['de']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['d']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['e']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['ef']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['efgh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['fgh']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['fg']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['f']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['g']) from system.numbers limit 129;
select 1 = multiSearchAny(materialize('abcdefgh'), ['gh']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abcdefgh'), ['h']) from system.numbers limit 129;

select 1 = multiSearchAny(materialize('abc'), ['b']);
select 1 = multiSearchAny(materialize('abc'), ['bc']);
select 0 = multiSearchAny(materialize('abc'), ['bcde']);
select 0 = multiSearchAny(materialize('abc'), ['bcdef']);
select 0 = multiSearchAny(materialize('abc'), ['bcdefg']);
select 0 = multiSearchAny(materialize('abc'), ['bcdefgh']);

select 0 = multiSearchAny(materialize('abc'), ['abcdefg']);
select 0 = multiSearchAny(materialize('abc'), ['abcdef']);
select 0 = multiSearchAny(materialize('abc'), ['abcde']);
select 0 = multiSearchAny(materialize('abc'), ['abcd']);
select 1 = multiSearchAny(materialize('abc'), ['abc']);
select 1 = multiSearchAny(materialize('abc'), ['ab']);
select 1 = multiSearchAny(materialize('abc'), ['a']);

select 1 = multiSearchAny(materialize('abcd'), ['c']);
select 1 = multiSearchAny(materialize('abcd'), ['cd']);
select 0 = multiSearchAny(materialize('abcd'), ['cde']);
select 0 = multiSearchAny(materialize('abcd'), ['cdef']);
select 0 = multiSearchAny(materialize('abcd'), ['cdefg']);
select 0 = multiSearchAny(materialize('abcd'), ['cdefgh']);

select 0 = multiSearchAny(materialize('abc'), ['defgh']);
select 0 = multiSearchAny(materialize('abc'), ['defg']);
select 0 = multiSearchAny(materialize('abc'), ['def']);
select 0 = multiSearchAny(materialize('abc'), ['de']);
select 0 = multiSearchAny(materialize('abc'), ['d']);


select 1 = multiSearchAny(materialize('abc'), ['b']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['bc']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcde']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcdef']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcdefg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['bcdefgh']) from system.numbers limit 10;


select 0 = multiSearchAny(materialize('abc'), ['abcdefg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['abcdef']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['abcde']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['abcd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['abc']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['ab']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['a']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abcd'), ['c']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abcd'), ['cd']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cde']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cdef']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cdefg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abcd'), ['cdefgh']) from system.numbers limit 10;

select 0 = multiSearchAny(materialize('abc'), ['defgh']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['defg']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['def']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['de']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('abc'), ['d']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('abc'), ['']);
select 1 = multiSearchAny(materialize('abc'), ['']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('abc'), ['']) from system.numbers limit 100;
select 1 = multiSearchAny(materialize('abc'), ['']) from system.numbers limit 1000;

select 1 = multiSearchAny(materialize('abab'), ['ab']);
select 1 = multiSearchAny(materialize('abababababababababababab'), ['abab']);
select 1 = multiSearchAny(materialize('abababababababababababab'), ['abababababababababa']);

-- select 'some random tests';

select [4, 1, 1, 2, 6, 1, 1, 0, 4, 1, 14, 0, 10, 0, 16, 6] = multiSearchAllPositions(materialize('jmdqwjbrxlbatqeixknricfk'), ['qwjbrxlba', 'jmd', '', 'mdqwjbrxlbatqe', 'jbrxlbatqeixknric', 'jmdqwjbrxlbatqeixknri', '', 'fdtmnwtts', 'qwjbrxlba', '', 'qeixknricfk', 'hzjjgrnoilfkvzxaemzhf', 'lb', 'kamz', 'ixknr', 'jbrxlbatq']) from system.numbers limit 10;
select [0, 0, 0, 2, 3, 0, 1, 0, 5, 0, 0, 0, 11, 10, 6, 7] = multiSearchAllPositions(materialize('coxcctuehmzkbrsmodfvx'), ['bkhnp', 'nlypjvriuk', 'rkslxwfqjjivcwdexrdtvjdtvuu', 'oxcctuehm', 'xcctuehmzkbrsm', 'kfrieuocovykjmkwxbdlkgwctwvcuh', 'coxc', 'lbwvetgxyndxjqqwthtkgasbafii', 'ctuehmzkbrsmodfvx', 'obzldxjldxowk', 'ngfikgigeyll', 'wdaejjukowgvzijnw', 'zkbr', 'mzkb', 'tuehm', 'ue']) from system.numbers limit 10;
select [1, 1, 0, 0, 0, 1, 1, 1, 4, 0, 6, 6, 0, 10, 1, 5] = multiSearchAllPositions(materialize('mpswgtljbbrmivkcglamemayfn'), ['', 'm', 'saejhpnfgfq', 'rzanrkdssmmkanqjpfi', 'oputeneprgoowg', 'mp', '', '', 'wgtljbbrmivkcglamemay', 'cbpthtrgrmgfypizi', 'tl', 'tlj', 'xuhs', 'brmivkcglamemayfn', '', 'gtljb']) from system.numbers limit 10;
select [1, 0, 0, 8, 6, 0, 7, 1, 3, 0, 0, 0, 0, 12] = multiSearchAllPositions(materialize('arbphzbbecypbzsqsljurtddve'), ['arbphzb', 'mnrboimjfijnti', 'cikcrd', 'becypbz', 'z', 'uocmqgnczhdcrvtqrnaxdxjjlhakoszuwc', 'bbe', '', 'bp', 'yhltnexlpdijkdzt', 'jkwjmrckvgmccmmrolqvy', 'vdxmicjmfbtsbqqmqcgtnrvdgaucsgspwg', 'witlfqwvhmmyjrnrzttrikhhsrd', 'pbzsqsljurt']) from system.numbers limit 10;
select [7, 0, 0, 8, 0, 2, 0, 0, 6, 0, 2, 0, 3, 1] = multiSearchAllPositions(materialize('aizovxqpzcbbxuhwtiaaqhdqjdei'), ['qpzcbbxuhw', 'jugrpglqbm', 'dspwhzpyjohhtizegrnswhjfpdz', 'pzcbbxuh', 'vayzeszlycke', 'i', 'gvrontcpqavsjxtjwzgwxugiyhkhmhq', 'gyzmeroxztgaurmrqwtmsxcqnxaezuoapatvu', 'xqpzc', 'mjiswsvlvlpqrhhptqq', 'iz', 'hmzjxxfjsvcvdpqwtrdrp', 'zovxqpzcbbxuhwtia', 'ai']) from system.numbers limit 10;
select [0, 0, 0, 19, 14, 22, 10, 0, 0, 13, 0, 8] = multiSearchAllPositions(materialize('ydfgiluhyxwqdfiwtzobwzscyxhuov'), ['srsoubrgghleyheujsbwwwykerzlqphgejpxvog', 'axchkyleddjwkvbuyhmekpbbbztxdlm', 'zqodzvlkmfe', 'obwz', 'fi', 'zsc', 'xwq', 'pvmurvrd', 'uulcdtexckmrsokmgdpkstlkoavyrmxeaacvydxf', 'dfi', 'mxcngttujzgtlssrmluaflmjuv', 'hyxwqdfiwtzobwzscyxhu']) from system.numbers limit 10;
select [6, 1, 1, 0, 0, 5, 1, 0, 8, 0, 5, 0, 2, 12, 0, 15, 0, 0] = multiSearchAllPositions(materialize('pyepgwainvmwekwhhqxxvzdjw'), ['w', '', '', 'gvvkllofjnxvcu', 'kmwwhboplctvzazcyfpxhwtaddfnhekei', 'gwainv', 'pyepgwain', 'ekpnogkzzmbpfynsunwqp', 'invmwe', 'hrxpiplfplqjsstuybksuteoz', 'gwa', 'akfpyduqrwosxcbdemtxrxvundrgse', 'yepgwainvmw', 'wekwhhqxxvzdjw', 'fyimzvedmyriubgoznmcav', 'whhq', 'ozxowbwdqfisuupyzaqynoprgsjhkwlum', 'vpoufrofekajksdp']) from system.numbers limit 10;
select [0, 0, 5, 1, 1, 0, 15, 1, 5, 10, 4, 0, 1, 0, 3, 0, 0, 0] = multiSearchAllPositions(materialize('lqwahffxurkbhhzytequotkfk'), ['rwjqudpuaiufle', 'livwgbnflvy', 'hffxurkbhh', '', '', 'xcajwbqbttzfzfowjubmmgnmssat', 'zytequ', 'lq', 'h', 'rkbhh', 'a', 'immejthwgdr', '', 'llhhnlhcvnxxorzzjt', 'w', 'cvjynqxcivmmmvc', 'wexjomdcmursppjtsweybheyxzleuz', 'fzronsnddfxwlkkzidiknhpjipyrcrzel']) from system.numbers limit 10;
select [0, 1, 11, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 1] = multiSearchAllPositions(materialize('nkddriylnakicwgdwrfxpodqea'), ['izwdpgrgpmjlwkanjrffgela', '', 'kicw', 'hltmfymgmrjckdiylkzjlvvyuleksikdjrg', 'yigveskrbidknjxigwilmkgyizewikh', 'xyvzhsnqmuec', 'odcgzlavzrwesjks', 'oilvfgliktoujukpgzvhmokdgkssqgqot', 'llsfsurvimbahwqtbqbp', 'nxj', 'pimydixeobdxmdkvhcyzcgnbhzsydx', 'couzmvxedobuohibgxwoxvmpote', 'driylnakicwgdwrf', 'nkddr']) from system.numbers limit 10;
select [0, 0, 0, 3, 0, 15, 0, 0, 12, 7, 0, 0, 0, 0, 5, 0] = multiSearchAllPositions(materialize('jnckhtjqwycyihuejibqmddrdxe'), ['tajzx', 'vuddoylclxatcjvinusdwt', 'spxkhxvzsljkmnzpeubszjnhqczavgtqopxn', 'ckhtjqwycyi', 'xlbfzdxspldoes', 'u', 'czosfebeznt', 'gzhabdsuyreisxvyfrfrkq', 'yihuejibqmd', 'jqwycyihuejibqm', 'cfbvprgzx', 'hxu', 'vxbhrfpzacgd', 'afoaij', 'htjqwycyihu', 'httzbskqd']) from system.numbers limit 10;
select [0, 0, 12, 4, 4, 0, 13, 23, 0, 1, 0, 2, 0, 0, 0, 3, 0, 0] = multiSearchAllPositions(materialize('dzejajvpoojdkqbnayahygidyrjmb'), ['khwxxvtnqhobbvwgwkpusjlhlzifiuclycml', 'nzvuhtwdaivo', 'dkqbnayahygidyr', 'jajvpoo', 'j', 'wdtbvwmeqgyvetu', 'kqbn', 'idyrjmb', 'tsnxuxevsxrxpgpfdgrkhwqpkse', '', 'efsdgzuefhdzkmquxu', 'zejajvpoojdkqbnayahyg', 'ugwfuighbygrxyctop', 'fcbxzbdugc', 'dxmzzrcplob', 'ejaj', 'wmmupyxrylvawsyfccluiiene', 'ohzmsqhpzbafvbzqwzftbvftei']) from system.numbers limit 10;
select [6, 8, 1, 4, 0, 10, 0, 1, 14, 0, 1, 0, 5, 0, 0, 0, 0, 15, 0, 1] = multiSearchAllPositions(materialize('ffaujlverosspbzaqefjzql'), ['lvero', 'erossp', 'f', 'ujlverosspbz', 'btfimgklzzxlbkbuqyrmnud', 'osspb', 'muqexvtjuaar', 'f', 'bzaq', 'lprihswhwkdhqciqhfaowarn', 'ffaujlve', 'uhbbjrqjb', 'jlver', 'umucyhbbu', 'pjthtzmgxhvpbdphesnnztuu', 'xfqhfdfsbbazactpastzvzqudgk', 'lvovjfoatc', 'z', 'givejzhoqsd', '']) from system.numbers limit 10;
select [5, 7, 0, 1, 6, 0, 0, 1, 1, 2, 0, 1, 4, 2, 0, 6, 0, 0] = multiSearchAllPositions(materialize('hzftozkvquknsahhxefzg'), ['ozkvquknsahhxefzg', 'kv', 'lkdhmafrec', '', 'zkvquknsahh', 'xmjuizyconipirigdmhqclox', 'dqqwolnkkwbyyjicsoshidbay', '', '', 'zf', 'sonvmkapcjcakgpejvn', 'hzftoz', 't', 'zftozkvqukns', 'dyuqohvehxsvdzdlqzl', 'zkvquknsahhx', 'vueohmytvmglqwptfbhxffspf', 'ilkdurxg']) from system.numbers limit 10;
select [1, 7, 6, 4, 0, 1, 0, 0, 0, 9, 7, 1, 1, 0, 0, 0] = multiSearchAllPositions(materialize('aapdygjzrhskntrphianzjob'), ['', 'jz', 'gjzrh', 'dygjzrhskntrphia', 'qcnahphlxmdru', '', 'rnwvzdn', 'isbekwuivytqggsxniqojrvpwjdr', 'sstwvgyavbwxvjojrpg', 'rhskn', 'jzrhskntrp', '', '', 'toilvppgjizaxtidizgbgygubmob', 'vjwzwpvsklkxqgeqqmtssnhlmw', 'znvpjjlydvzhkt']) from system.numbers limit 10;
select [0, 1, 0, 1, 0, 0, 10, 0, 0, 0, 11, 0, 5, 0] = multiSearchAllPositions(materialize('blwpfdjjkxettfetdoxvxbyk'), ['wgylnwqcrojacofrcanjme', 'bl', 'qqcunzpvgi', '', 'ijemdmmdxkakrawwdqrjtrttig', 'qwkaifalc', 'xe', 'zqocnfuvzowuqkmwrfxw', 'xpaayeljvly', 'wvphqqhulpepjjjnxjfudfcomajc', 'ettfetdoxvx', 'ikablovwhnbohibbuhwjshhdemidgreqf', 'fdjjkxett', 'kiairehwbxveqkcfqhgopztgpatljgqp']) from system.numbers limit 10;
select [0, 0, 6, 1, 1, 0, 0, 1, 2, 0, 0, 0, 0, 0] = multiSearchAllPositions(materialize('vghzgedqpnqtvaoonwsz'), ['mfyndhucfpzjxzaezny', 'niejb', 'edqpnqt', '', 'v', 'kivdvealqadzdatziujdnvymmia', 'lvznmgwtlwevcxyfbkqc', 'vghzge', 'gh', 'tbzle', 'vjiqponbvgvguuhqdijbdeu', 'mshlyabasgukboknbqgmmmj', 'kjk', 'abkeftpnpvdkfyrxbrihyfxcfxablv']) from system.numbers limit 10;
select [0, 0, 0, 0, 9, 0, 7, 0, 9, 8, 0, 0] = multiSearchAllPositions(materialize('oaghnutqsqcnwvmzrnxgacsovxiko'), ['upien', 'moqszigvduvvwvmpemupvmmzctbrbtqggrk', 'igeiaccvxejtfvifrmimwpewllcggji', 'wnwjorpzgsqiociw', 'sq', 'rkysegpoej', 'tqsqcnwvmzrnxgacsovxiko', 'ioykypvfjufbicpyrpfuhugk', 's', 'qsqcnwvmzrnxgacsov', 'hhbeisvmpnkwmimgyfmybtljiu', 'kfozjowd']) from system.numbers limit 10;
select [0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 1, 20, 5, 0, 0, 14, 1, 1, 0, 0] = multiSearchAllPositions(materialize('wbjfsevqspsvbwlzrkhcfuhxddbq'), ['ltgjbz', 's', 'qdfnmggupdfxjfnmvwyrqopxtxf', 'sazlkmaikcltojbzbmdfddu', 'yzanifqxufyfwrxzkhngoxkrrph', 'iwskc', 'xkykshryphyfnwcnmjfqjrixykmzmwm', 'wwpenztbhkdbwidfkypqlxivsjs', 'rlkevy', 'qigywtkezwd', '', 'c', 'sevqspsvbwlzrk', 'gwg', 'iduhrjsrtodxdkjykjoghtjtvplrscitxnvt', 'wlzrkhcfuhxddb', '', 'wbjfsev', 'zytusrcvqbazb', 'tec']) from system.numbers limit 10;
select [0, 1, 5, 0, 6, 8, 0, 3, 2, 0, 0, 9, 0, 4, 0, 0] = multiSearchAllPositions(materialize('mxiifpzlovgfozpgirtio'), ['srullnscuzenzhp', '', 'f', 'apetxezid', 'pzlovgf', 'lo', 'ecbmso', 'i', 'xiifpzlovgfozpgir', 'bnefwypvctubvslsesnctqspdyctq', 'tdncmgbikboss', 'o', 'zmgobcarxlxaho', 'ifpzlovgfozpg', 'dwmjqyylvsxzfr', 'pxhrecconce']) from system.numbers limit 10;
select [0, 0, 0, 2, 0, 0, 2, 0, 8, 0, 0, 0, 7, 0, 0, 0, 21, 3, 1, 8] = multiSearchAllPositions(materialize('jtvnrdpdevgnzexqdrrxqgiujexhm'), ['ibkvzoqmiyfgfztupug', 'iqzeixfykxcghlbgsicxiywlurrgjsywwk', 'vzdffjzlqxgzdcrkgoro', 'tvnrdpdevgnzexqdr', 'nqywueahcmoojtyjlhfpysk', 'iqalixciiidvrtmpzozfb', 'tv', 'rxkfeasoff', 'devgnzexqdrrxqgiuj', 'kvvuvyplboowjrestyvdfrxdjjujvkxy', 'shkhpneekuyyqtxfxutvz', 'yy', 'pdevgnz', 'nplpydxiwnbvlhoorcmqkycqisi', 'jlkxplbftfkxqgnqnaw', 'qdggpjenbrwbjtorbi', 'qgiuje', 'vnrdpd', '', 'dev']) from system.numbers limit 10;
select [14, 0, 0, 7, 20, 6, 0, 13, 0, 0, 20, 0, 20, 2, 0, 8, 2, 11, 2, 0] = multiSearchAllPositions(materialize('asjwxabjrwgcdviokfaoqvqiafz'), ['v', 'zqngytligwwpzxhatyayvdnbbj', 'gjicovfzgbyagiirn', 'bjrwgcdviok', 'oqvqiafz', 'abjrwgc', 'wulrpfzh', 'dviokfao', 'esnchjuiufjadqmdtrpcd', 'tkodqzsjchpaftk', 'oqvq', 'eyoshlrlvmnqjmtmloryvg', 'oqv', 'sjwx', 'uokueelyytnoidplwmmox', 'jrwgcdviokfaoqvqiaf', 'sjwxabjrwgcdviokfaoqvqi', 'gcdviokfa', 'sjwxab', 'zneabsnfucjcwauxmudyxibnmxzfx']) from system.numbers limit 10;
select [0, 16, 8, 0, 10, 0, 0, 0, 0, 1, 0, 6, 0, 1, 0, 4, 0, 6, 0, 0] = multiSearchAllPositions(materialize('soxfqagiuhkaylzootfjy'), ['eveprzxphyenbrnnznpctvxn', 'oo', 'iuhka', 'ikutjhrnvzfb', 'h', 'duyvvjizristnkczgwj', 'ihfrp', 'afpyrlj', 'uonp', 'soxfqagiuhkaylzootfjy', 'qeckxkoxldpzzpmkbvcex', 'agiuhkaylzo', 'tckcumkbsgrgqjvtlijack', '', 'fnfweqlldcdnwfaohqohp', 'fqagiuhkayl', 'pqnvwprxwwrcjqvfsbfimwye', 'agi', 'ta', 'r']) from system.numbers limit 10;
select [3, 7, 1, 6, 0, 1, 0, 11, 0, 9, 17, 1, 18, 12] = multiSearchAllPositions(materialize('ladbcypcbcxahmujwezkvweud'), ['db', 'pcbcxahm', 'lad', 'ypcb', 'atevkzyyxhphtuekymhh', 'lad', 'mltjrwaibetrtwpfa', 'xahmujwezkvweud', 'dg', 'bcxahmujw', 'we', '', 'e', 'ahmujwezkvw']) from system.numbers limit 10;
select [6, 0, 11, 0, 7, 0, 0, 0, 6, 1, 0, 3, 0, 0, 0, 0] = multiSearchAllPositions(materialize('hhkscgmqzmuwltmrhtxnnzsxl'), ['gmqzmuwltmrh', 'qtescwjubeqhurqoqfjauwxdoc', 'uwltmrh', 'qlhyfuspwdtecdbrmrqcnxghhlnbmzs', 'm', 'kcsuocwokvohnqonnfzmeiqtomehksehwc', 'hoxocyilgrxxoek', 'nisnlmbdczjsiw', 'gmqz', '', 'cqzz', 'k', 'utxctwtzelxmtioyqshxedecih', 'ifsmsljxzkyuigdtunwk', 'ojxvxwdosaqjhrnjwisss', 'dz']) from system.numbers limit 10;
select [0, 0, 19, 7, 0, 0, 1, 0, 0, 12, 0, 0, 1, 0, 1, 1, 5, 0, 23, 8] = multiSearchAllPositions(materialize('raxgcqizulxfwivauupqnofbijxfr'), ['sxvhaxlrpviwuinrcebtfepxxkhxxgqu', 'cuodfevkpszuimhymxypktdvicmyxm', 'pqnof', 'i', 'ufpljiniflkctwkwcrsbdhvrvkizticpqkgvq', 'osojyhejhrlhjvqrtobwthjgw', '', 'anzlevtxre', 'ufnpkjvgidirrnpvbsndfnovebdily', 'fwivauupqnofbi', 'rywyadwcvk', 'ltnlhftdfefmkenadahcpxw', '', 'xryluzlhnsqk', 'r', '', 'cqizulxfwivauupqnofb', 'y', 'fb', 'zulxfwivauupqnofbijxf']) from system.numbers limit 10;
select [4, 0, 0, 0, 0, 24, 1, 2, 0, 2, 0, 0, 8, 0] = multiSearchAllPositions(materialize('cwcqyjjodlepauupgobsgrzdvii'), ['q', 'yjppewylsqbnjwnhokzqtauggsjhhhkkkqsy', 'uutltzhjtc', 'pkmuptmzzeqhichaikwbggronli', 'erzgcuxnec', 'dvii', '', 'w', 'fkmpha', 'wcqyjjodlepauupgobsgrz', 'cbnmwirigaf', 'fcumlot', 'odlepauu', 'lthautlklktfukpt']) from system.numbers limit 10;
select [1, 1, 1, 1, 22, 0, 0, 8, 18, 15] = multiSearchAllPositions(materialize('vpscxxibyhvtmrdzrocvdngpb'), ['', '', '', '', 'n', 'agrahemfuhmftacvpnaxkx', 'dqqwvfsrqv', 'byhvtmrdzrocv', 'ocvdn', 'dzrocvdngpb']) from system.numbers limit 10;
select [1, 1, 1, 15, 10, 0, 0, 0, 0, 2] = multiSearchAllPositions(materialize('nfoievsrpvheprosjdsoiz'), ['', 'nfo', '', 'osjd', 'vheprosjdsoiz', 'az', 'blhvdycvjnxaipvxybs', 'umgxmpkvuvuvdaczkz', 'gfspmnzidixcjgjw', 'f']) from system.numbers limit 10;
select [0, 0, 2, 2, 0, 0, 0, 11, 10, 4, 9, 1, 6, 4, 0, 0] = multiSearchAllPositions(materialize('bdmfwdisdlgbcidshnhautsye'), ['uxdceftnmnqpveljer', 'xdnh', 'dmf', 'dmfwdisdlgbc', 'cpwnaijpkpyjgaq', 'doquvlrzhusjbxyqcqxvwr', 'llppnnmtqggyfoxtawnngsiiunvjjxxsufh', 'gbcidshnhau', 'lgbcids', 'f', 'dlgbc', 'bdmfwdisdlgbcids', 'disdlgbcidshnhautsy', 'fwdisdlgbcidshn', 'zfpbfc', 'triqajlyfmxlredivqiambigmge']) from system.numbers limit 10;
select [0, 0, 16, 0, 0, 0, 14, 6, 2, 1, 0, 0, 1, 0, 10, 12, 0, 0, 0, 0] = multiSearchAllPositions(materialize('absimumlxdlxuzpyrunivcb'), ['jglfzroni', 'wzfmtbjlcdxlbpialqjafjwz', 'yrun', 'fgmljkkp', 'nniob', 'fdektoyhxrumiycvkwekphypgti', 'zp', 'um', 'bsimu', '', 'yslsnfisaebuujltpgcskhhqcucdhb', 'xlaphsqgqsfykhilddctrawerneqoigb', '', 'pdvcfxdlurmegspidojt', 'd', 'xu', 'fdp', 'xjrqmybmccjbjtvyvdh', 'nvhdfatqi', 'neubuiykajzcrzdbvpwjhlpdmd']) from system.numbers limit 10;
select [0, 0, 0, 9, 0, 0, 1, 1, 1, 1] = multiSearchAllPositions(materialize('lvyenvktdnylszlypuwqecohy'), ['ihlsiynj', 'ctcnhbkumvbgfdclwjhsswpqyfrx', 'rpgqwkydwlfclcuupoynwrfffogxesvmbj', 'dnyl', 'coeqgdtbemkhgplprfxgwpl', 'dkbshktectbduxlcaptlzspq', 'l', 'lvyenvktdnylszlypuw', 'lvyenvk', '']) from system.numbers limit 10;
select [1, 0, 0, 0, 0, 1, 2, 22, 8, 17, 1, 13, 0, 0, 0, 0, 0, 5] = multiSearchAllPositions(materialize('wphcobonpgaqwgfenotzadgqezx'), ['', 'qeuycfhkfjwokxgrkaodqioaotkepzlhnrv', 'taehtytq', 'gejlcipocalc', 'poyvvvntrvqazixkwigtairjvxkgouiuva', '', 'phc', 'dg', 'npgaqwg', 'notzadgqe', '', 'wgfe', 'smipuxgvntys', 'qhrfdytbfeujzievelffzrv', 'cfmzw', 'hcywnyguzjredwjbqtwyuhtewuhzkc', 'tssfeinoykdauderpjyxtmb', 'obonpgaqwgfen']) from system.numbers limit 10;
select [0, 0, 0, 0, 0, 6, 6, 0, 0, 2, 0, 5, 2, 0, 6, 3] = multiSearchAllPositions(materialize('qvslufpsddtfudzrzlvrzdra'), ['jxsgyzgnjwyd', 'hqhxzhskwivpuqkjheywwfhthm', 'kbwlwadilqhgwlcpxkadkamsnzngms', 'fxunda', 'nlltydufobnfxjyhch', 'fpsddtfudzrzl', 'fp', 'ykhxjyqtvjbykskbejpnmbxpumknqucu', 'iyecekjcbkowdothxc', 'vslufpsddtfu', 'mjgtofkjeknlikrugkfhxlioicevil', 'uf', 'vslufpsdd', 'cxizdzygyu', 'fpsddtfudzrz', 'slufp']) from system.numbers limit 10;
select [12, 0, 0, 0, 0, 1, 6, 0, 1, 2] = multiSearchAllPositions(materialize('ydsbycnifbcforymknzfi'), ['forymkn', 'vgxtcdkfmjhc', 'ymugjvtmtzvghmifolzdihutqoisl', 'fzooddrlhi', 'bdefmxxdepcqi', '', 'cnif', 'ilzbhegpcnkdkooopaguljlie', '', 'dsbycnifbcforym']) from system.numbers limit 10;
select [0, 2, 4, 1, 1, 3, 0, 0, 0, 7] = multiSearchAllPositions(materialize('sksoirfwdhpdyxrkklhc'), ['vuixtegnp', 'ks', 'oirfwdhpd', 'sksoirf', 'skso', 'soi', 'eoxpa', 'vpfmzovgatllf', 'txsezmqvduxbmwu', 'fw']) from system.numbers limit 10;
select [2, 21, 8, 10, 6, 0, 1, 11, 0, 0, 21, 4, 29, 0] = multiSearchAllPositions(materialize('wlkublfclrvgixpbvgliylzbuuoyai'), ['l', 'ylzbuu', 'clr', 'rvgi', 'lf', 'bqtzaqjdfhvgddyaywaiybk', '', 'vgixpbv', 'ponnohwdvrq', 'dqioxovlbvobwkgeghlqxtwre', 'y', 'ublfclrvgix', 'a', 'eoxxbkaawwsdgzfweci']) from system.numbers limit 10;
select [0, 0, 2, 1, 1, 9, 1, 0, 0, 1] = multiSearchAllPositions(materialize('llpbsbgmfiadwvvsciak'), ['knyjtntotuldifbndcpxzsdwdduv', 'lfhofdxavpsiporpdyfziqzcni', 'lpbsbgmf', 'llpbsbgmfi', 'llpbsbgmfiadwvv', 'fia', '', 'uomksovcuhfmztuqwzwchmwvonk', 'ujbasmokvghmredszgwe', '']) from system.numbers limit 10;
select [3, 0, 0, 0, 6, 1, 7, 0, 2, 1, 1, 0, 7, 0, 1, 0, 1, 1, 5, 11] = multiSearchAllPositions(materialize('hnmrouevovxrzrejesigfukkmbiid'), ['m', 'apqlvipphjbui', 'wkepvtnpu', 'amjvdpudkdsddjgsmzhzovnwjrzjirdoxk', 'ue', '', 'evov', 'qoplzddxjejvbmthnplyha', 'nmrouevovxrz', '', 'hnmrouev', 'hnzevrvlmxnjmvhitgdhgd', 'evovxrzrejesig', 'yvlxrjaqdaizishkftgcuikt', '', 'buyrmbkvqukochjteumqchrhxgtmuorsdgzlfn', '', 'hnmrouevov', 'ouevovx', 'xr']) from system.numbers limit 10;
select [0, 13, 0, 0, 0, 0, 0, 14, 0, 0, 1, 12, 0, 1] = multiSearchAllPositions(materialize('uwfgpemgdjimotxuxrxxoynxoaw'), ['uzcevfdfy', 'otxuxrxxoynxoa', 'xeduvwhrogxccwhnzkiolksry', 'pxdszcyzxlrvkymhomz', 'vhsacxoaymycvcevuujpvozsqklahstmvgt', 'zydsajykft', 'vdvqynfhlhoilkhjjkcehnpmwgdtfkspk', 'txuxrx', 'slcaryelankprkeyzaucfhe', 'iocwevqwpkbrbqvddaob', 'uwfg', 'motxuxrxx', 'kpzbg', '']) from system.numbers limit 10;
select [1, 1, 0, 6, 6, 0, 0, 0, 8, 0, 8, 14, 1, 5, 6, 0, 0, 1] = multiSearchAllPositions(materialize('epudevopgooprmhqzjdvjvqm'), ['ep', 'epudevopg', 'tlyinfnhputxggivtyxgtupzs', 'vopgoop', 'v', 'hjfcoemfk', 'zjyhmybeuzxkuwaxtcut', 'txrxzndoxyzgnzepjzagc', 'pgooprmhqzj', 'wmtqcbsofbe', 'pgo', 'm', '', 'evopgooprmhqzjdv', 'vopgooprmhqzjdv', 'gmvqubpsnvrabixk', 'wjevqrrywloomnpsjbuybhkhzdeamj', '']) from system.numbers limit 10;
select [15, 4, 4, 0, 0, 1, 1, 0, 0, 0, 0, 20, 0, 10, 1, 1, 0, 2, 4, 3] = multiSearchAllPositions(materialize('uogsfbdefogwnekfoeobtkrgiceksz'), ['kfoeobtkrgice', 'sfbd', 'sfbdefogwn', 'zwtenhiqavmqoolkvjiqjfb', 'vnjkshyvpwhrauackplqllakcjyamvsuokrxbfv', 'uog', '', 'qtzuhdcdymytgtscvzlzswdlrqidreuuuqk', 'vlridmjlbxyiljpgxsctzygzyawqqysf', 'xsnkwyrmjaaaryvrdgtoshdxpvgsjjrov', 'fanchgljgwosfamgscuuriwospheze', 'btkrgicek', 'ohsclekvizgfoatxybxbjoxpsd', 'ogwnekfoeobtkr', '', '', 'vtzcobbhadfwubkcd', 'og', 's', 'gs']) from system.numbers limit 10;
select [0, 0, 5, 1, 0, 5, 1, 6, 0, 1, 9, 0, 1, 1] = multiSearchAllPositions(materialize('aoiqztelubikzmxchloa'), ['blc', 'p', 'ztelubikzmxchlo', 'aoiqztelubi', 'uckqledkyfboolq', 'ztelubikzmxch', 'a', 'telubikzm', 'powokpdraslpadpwvrqpbb', 'aoiqztelu', 'u', 'kishbitagsxnhyyswn', '', '']) from system.numbers limit 10;
select [5, 11, 0, 0, 0, 5, 0, 0, 0, 1, 16, 0, 0, 0, 0, 0] = multiSearchAllPositions(materialize('egxmimubhidowgnfziwgnlqiw'), ['imubhidowgnfzi', 'dowgnf', 'yqpcpfvnfpxetozraxbmzxxcvtzm', 'xkbaqvzlqjyjoiqourezbzwaqkfyekcfie', 'jjctusdmxr', 'imubhi', 'zawnslbfrtqohnztmnssxscymonlhkitq', 'oxcitennfpuoptwrlmc', 'ac', 'egxmi', 'fziwgn', 'rt', 'fuxfuctdmawmhxxxg', 'suulqkrsfgynruygjckrmizsksjcfwath', 'slgsq', 'zcbqjpehilwyztumebmdrsl']) from system.numbers limit 10;
select [20, 0, 9, 0, 0, 14, 0, 5, 8, 3, 0, 0, 0, 4] = multiSearchAllPositions(materialize('zczprzdcvcqzqdnhubyoblg'), ['obl', 'lzrjyezgqqoiydn', 'vc', 'nbvwfpmqlziedob', 'pnezljnnujjbyviqsdpaqkkrlogeht', 'dn', 'irvgeaq', 'rzdcvcqzqdnh', 'cvcqzqdnh', 'zprzdcv', 'wvvgoexuevmqjeqavsianoviubfixdpe', 'aeavhqipsvfkcynyrtlxwpegwqmnd', 'blckyiacwgfaoarfkptwcei', 'prz']) from system.numbers limit 10;
select [2, 1, 1, 9, 10, 5, 0, 0, 0, 2, 9, 7, 9, 0, 1, 9, 7, 0] = multiSearchAllPositions(materialize('mvovpvuhjwdzjwojcxxrbxy'), ['vo', '', '', 'jwdz', 'wdzj', 'pvu', 'ocxprubxhjnji', 'phzfbtacrg', 'jguuqhhxbrwbo', 'vovpvuhjwd', 'jw', 'u', 'jwdzjwojcx', 'nlwfvolaklizslylbvcgicbjw', '', 'jwd', 'uhjwdz', 'bbcsuvtru']) from system.numbers limit 10;
select [2, 0, 21, 0, 0, 0, 3, 0, 0, 0, 0, 10, 1, 18] = multiSearchAllPositions(materialize('nmdkwvafhcbipwoqtsrzitwxsnabwf'), ['m', 'ohlfouwyucostahqlwlbkjgmdhdyagnihtmlt', 'itwx', 'jjkyhungzqqyzxrq', 'abkqvxxpu', 'lvzgnaxzctaarxuqowcski', 'dkwvafhcb', 'xuxjexmeeqvyjmpznpdmcn', 'vklvpoaakfnhtkprnijihxdbbhbllnz', 'fpcdgmcrwmdbflnijjmljlhtkszkocnafzaubtxp', 'hmysdmmhnebmhpjrrqpjdqsgeuutsj', 'cbipwoqtsrzitwxsna', 'nm', 'srzitwx']) from system.numbers limit 10;
select [17, 5, 0, 13, 0, 0, 10, 1, 0, 19, 10, 8, 0, 4] = multiSearchAllPositions(materialize('gfvndbztroigxfujasvcdgfbh'), ['asvcdgf', 'dbztroigxfujas', 'pr', 'xfujas', 'nxwdmqsobxgm', 'wdvoepclqfhy', 'oigxfu', '', 'flgcghcfeiqvhvqiriciywbkhrxraxvneu', 'vcd', 'oigxfu', 'troigxfuj', 'gbnyvjhptuehkefhwjo', 'ndbz']) from system.numbers limit 10;
select [0, 14, 1, 0, 0, 1, 1, 11, 0, 8, 6, 0, 3, 19, 7, 0] = multiSearchAllPositions(materialize('nofwsbvvzgijgskbqjwyjmtfdogzzo'), ['kthjocfzvys', 'skbqjwyjmtfdo', 'nof', 'mfapvffuhueofutby', 'vqmkgjldhqohipgecie', 'nofwsbv', '', 'ijgs', 'telzjcbsloysamquwsoaso', 'vzgijgskbqjwyjmt', 'bvvzgijgskbqjwyjmtfd', 'hdlvuoylcmoicsejofcgnvddx', 'fwsbvvzgijgskb', 'wyjm', 'vvzgijg', 'fwzysuvkjtdiufetvlfwf']) from system.numbers limit 10;
select [10, 2, 13, 0, 0, 0, 2, 0, 9, 2, 4, 1, 1, 0, 1, 6] = multiSearchAllPositions(materialize('litdbgdtgtbkyflsvpjbqwsg'), ['tbky', 'itdbgdtgtb', 'yflsvpjb', 'ikbylslpoqxeqoqurbdehlroympy', 'hxejlgsbthvjalqjybc', 'sontq', 'itdbgd', 'ozqwgcjqmqqlkiaqppitsvjztwkh', 'gtbkyf', 'itdbgdtgtbkyfls', 'dbg', 'litdb', '', 'qesbakrnkbtfvwu', 'litd', 'g']) from system.numbers limit 10;
select [0, 0, 1, 1, 5, 0, 8, 12, 0, 2, 0, 7, 0, 6] = multiSearchAllPositions(materialize('ijzojxumpvcxwgekqimrkomvuzl'), ['xirqhjqibnirldvbfsb', 'htckarpuctrasdxoosutyxqioizsnzi', '', '', 'jxu', 'dskssv', 'mpvcxwgekqi', 'xwgek', 'qsuexmzfcxlrhkvlzwceqxfkyzogpoku', 'jzojx', 'carjpqihtpjniqz', 'umpvcxwgekq', 'krpkzzrxxtvfhdopjpqcyxfnbas', 'xumpvcxwg']) from system.numbers limit 10;
select [0, 0, 0, 6, 0, 8, 0, 2, 0, 0, 0, 0, 14, 0, 0, 1, 1, 0, 0, 0] = multiSearchAllPositions(materialize('zpplelzzxsjwktedrrtqhfmoufv'), ['jzzlntsokwlm', 'cb', 'wuxotyiegupflu', 'lzzxsjwkte', 'owbxgndpcmfuizpcduvucnntgryn', 'zxsjwktedrrtqhf', 'kystlupelnmormqmqclgjakfwnyt', 'pple', 'lishqmxa', 'mulwlrbizkmtbved', 'uchtfzizjiooetgjfydhmzbtmqsyhayd', 'hrzgjifkinwyxnazokuhicvloaygeinpd', 'tedrrt', 'shntwxsuxux', 'evrjehtdzzoxkismtfnqp', 'z', '', 'nxtybut', 'vfdchgqclhxpqpmitppysbvxepzhxv', 'wxmvmvjlrrehwylgqhpehzotgrzkgi']) from system.numbers limit 10;

select [15, 19, 0, 0, 15, 0, 0, 1, 2, 6] = multiSearchAllPositionsUTF8(materialize('зжерхмчсйирдчрришкраоддцфгх'), ['ришкра', 'раоддц', 'фттиалусгоцжлтщзвумрдчи', 'влййи', 'ришкра', 'цгфжуцгивй', 'ккгжхрггчфглх', 'з', 'жерхмчсйи', 'мчсйирдчрришкраоддц']) from system.numbers limit 10;
select [0, 0, 0, 1, 4, 0, 14, 0, 1, 8, 8, 9, 0, 0, 4, 0] = multiSearchAllPositionsUTF8(materialize('етвхйчдобкчукхпщлмжпфайтфдоизщ'), ['амфшужперосрфщфлижйййжжжй', 'ххкбщшзлмщггтшцпсдйкдшйвхскемц', 'ергйплгпнглккшкарещимгапхг', '', 'хйчдо', 'вввбжовшзйбгуоиждепйабаххеквщижтйиухос', 'хпщл', 'жфуомщуххнедзхищнгхрквлпмзауеегз', 'етвхй', 'о', 'о', 'бк', 'цфецккифж', 'аизлокл', 'х', 'слщгеивлевбчнчбтшгфмжрфка']) from system.numbers limit 10;
select [0, 0, 1, 2, 0, 0, 14, 0, 3, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('йбемооабурнирйофшдгпснж'), ['гпфцл', 'нчбперпмцкввдчсщвзйрдфнф', '', 'бем', 'ч', 'жгш', 'йофшдгпснж', 'шасгафчг', 'емооабур', 'пиохцжццгппщчопзйлмуотз', 'рпдомнфвопхкшешйишумбацтл', 'нисиийфррбдоц']) from system.numbers limit 10;
select [1, 18, 12, 0, 0, 1, 1, 3, 7, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('гсщнфийтфзжцйпфбйалущ'), ['', 'алущ', 'цйпфбйал', 'цвбфцйвсвлицсчнргпцнр', 'х', 'гс', '', 'щн', 'й', 'дгйрвцщтп', 'уитвквоффвцхфишрлерйцувф', 'кфтййлпнзжчижвглзкижн']) from system.numbers limit 10;
select [14, 0, 5, 5, 0, 6, 0, 16, 0, 0] = multiSearchAllPositionsUTF8(materialize('ефщнйнуйебнснлрцгкеитбг'), ['лрцгкеитб', 'епклжфцпнфопе', 'йнуйебн', 'й', 'тлт', 'нуйебнснлрцгкеит', 'глечршгвотумкимтлм', 'цгк', 'щгйчой', 'звкцкчк']) from system.numbers limit 10;
select [0, 1, 18, 6, 0, 3, 0, 0, 25, 0, 0, 1, 16, 5, 1, 7, 0, 0] = multiSearchAllPositionsUTF8(materialize('пумгмцшмжштсшлачсжарерфиозиг'), ['чсуубфийемквмоотванухмбрфхжоест', '', 'жар', 'цшмжш', 'жртещтинтвпочнкдткцза', 'м', 'адзгтбаскщгдшжл', 'штфжшллезпджигщфлезфгзчайанхктицштйй', 'о', 'етадаарйсцейдошшцечхзлшлрртсрггцртспд', 'зтвшалрпфлщбцд', 'пу', 'ч', 'мцшмжштсшлачсж', '', 'шмжшт', 'ещтжшйтчзчаноемрбц', 'тевбусешйрйчшзо']) from system.numbers limit 10;
select [7, 10, 0, 0, 0, 0, 1, 12, 9, 2, 0, 0, 0, 4, 1, 1, 0, 6] = multiSearchAllPositionsUTF8(materialize('дупгвндвйжмаузнллнзл'), ['двйжмаузн', 'жмаузнлл', 'емйжркоблновцгпезрдавкбелцщста', 'щзкгм', 'лебрпцрсутшриащгайвц', 'лзнмл', 'д', 'ауз', 'йжмау', 'упгвндвйж', 'жщсббфвихг', 'всигсеигцбгаелтчкирлнзшзцжещнс', 'рмшиеиесрлщципщхкхтоцщчйоо', 'гвн', '', '', 'йадеоцлпшпвщзещзкхйрейопмажбб', 'ндв']) from system.numbers limit 10;
select [0, 0, 0, 8, 3, 10, 22, 0, 13, 11, 0, 1, 18, 0, 1, 0] = multiSearchAllPositionsUTF8(materialize('жшзфппавввслфцлнщшопкдшку'), ['саоткнхфодзаа', 'кйхванкзаисйбврщве', 'бчоуучватхфукчф', 'вввслфц', 'з', 'вслфцлнщшопк', 'дшк', 'из', 'фцл', 'с', 'зртмцтпощпщхк', 'жшзфппавввслфц', 'шопк', 'збтхрсдтатхпрзлхдооощифачхчфн', '', 'жщшийугз']) from system.numbers limit 10;
select [2, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 21, 0, 6, 0] = multiSearchAllPositionsUTF8(materialize('пчботухвгдчекмжндбоожш'), ['чботухвгдчекмжндб', 'от', 'гвсжжйлбтщчучнхсмдйни', 'жцжзмшлибшефуоуомпацбщщу', 'онхфлуцйлхтбмц', 'йтепжу', 'хтдрпвкщрли', 'аддайф', 'нхегщккбфедндоацкиз', 'йгкцзртфжгв', 'буелрщмхйохгибжндфшщвшрлдччрмфмс', 'цщцтзфнщ', 'уч', 'пчб', 'жш', 'пнфббтшйгхйрочнлксщпгвжтч', 'ухвг', 'лсцппузазщрйхймщбзоршощбзленхп']) from system.numbers limit 10;
select [0, 0, 4, 11, 0, 0, 0, 0, 0, 11, 2, 4, 6, 0, 0, 1, 2, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('тжрмчпваухрхуфбгнифгбопфт'), ['дпмгкекщлнемссаицщпащтиуцхкфчихтц', 'акйиуоатунтчф', 'мчпва', 'рхуфбгнифгб', 'кнаишж', 'пчвотенеафкухжцешбцхг', 'опеа', 'ушчадфтчхечеуркбтел', 'ашшптаударчжчмвалтдхкимищпф', 'рхуфбгниф', 'ж', 'мчпваухрхуфбгнифг', 'пваухрху', 'зргачбтцдахвймсбсврбндзтнущхвп', 'асбфцавбгуолг', 'тж', 'жрмчпваухрх', 'мрвзцгоб', 'чрцснчсдхтзжвнздзфцвхеилишдбж', 'кчт']) from system.numbers limit 10;
select [0, 2, 4, 0, 6, 0, 0, 0, 0, 19, 7, 1, 0, 1, 0, 0, 2, 10, 0, 1] = multiSearchAllPositionsUTF8(materialize('опрурпгабеарушиойцрхвбнсщ'), ['йошуоесдщеж', 'пр', 'урпгабеарушиой', 'хщиаршблашфажщметчзи', 'пгабеарушиойцрхвб', 'щцбдвц', 'еечрззвкожзсдурйщувмцйшихдц', 'офхачгсзашфзозрлба', 'айдфжджшжлрргмабапткбцпиизигдтс', 'рх', 'габ', '', 'цнкдбфчщшмчулврбцчакщвзхлазфа', '', 'екбтфпфилсаванхфкмчнпумехиищди', 'епвщхаклшомвцжбф', 'прурпгабе', 'еарушиойцрхв', 'црвтгрзтитц', 'опрурпг']) from system.numbers limit 10;
select [0, 10, 1, 0, 0, 0, 0, 0, 10, 0, 15, 2] = multiSearchAllPositionsUTF8(materialize('угпщлзчжшбзвууцшатпщцр'), ['цоуарцжсз', 'бз', '', 'пщфтзрч', 'лфуипмсдмнхнгйнтк', 'айжунцйбйцасчфдхй', 'щдфщлцптплсачв', 'грв', 'бзвууц', 'бумййшдшфашцгзфвчвзвтсувнжс', 'цшатпщ', 'гпщлзчжшб']) from system.numbers limit 10;
select [0, 15, 0, 1, 5, 0, 0, 5, 0, 0, 0, 1, 0, 0] = multiSearchAllPositionsUTF8(materialize('цнлеодлмдцдйснитвдчтхжизв'), ['ивкчсзшугоцжчохщцабл', 'итвдчт', 'кнх', '', 'одлм', 'ктшфзбщзцуймагсоукщщудвуфо', 'ххеаефудгчхр', 'одлмдцдйснитвдчт', 'умцлпкв', 'зщсокйтцзачщафвбповжгнлавсгйг', 'бкибм', '', 'охсоихнцчцшевчеележтука', 'фаийхгжнсгищгщц']) from system.numbers limit 10;
select [0, 0, 0, 2, 0, 0, 0, 0, 3, 2, 3, 6, 0, 0, 0, 12, 4, 1] = multiSearchAllPositionsUTF8(materialize('бгдбувдужщвоошлтчрбй'), ['щвбаиф', 'итчднесжкчжвпжйвл', 'мм', 'г', 'хктзгтзазфгщшфгбеулцмдмдбдпчзх', 'сфуак', 'злйфцощегзекщб', 'фшлдтолрщфзжчмих', 'дбувдужщ', 'гдб', 'дбувдужщ', 'в', 'лчищкечнжщисцичбнзшмулпмлп', 'чжцсгмгфвлиецахзнрбмщин', 'обпжвй', 'о', 'бувдужщвоош', '']) from system.numbers limit 10;
select [0, 2, 5, 3, 2, 0, 1, 0, 0, 4, 2, 0, 0, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('шсушлорзфжзудбсейенм'), ['чнзпбновтршеумбвщчлх', 'су', 'лорзфж', 'ушлорзфжзудб', 'сушлорзфжзудбсейенм', 'ткдрхфнб', '', 'пщд', 'чбдцмщ', 'шлорзфж', 'су', 'сккигркедчожжемгнайвйчтдмхлтти', 'мц', 'пхнхрхйцйсйбхчлктз', 'иафжстлйфцр', 'алщщлангнбнйхлшлфшйонщек']) from system.numbers limit 10;
select [12, 1, 0, 5, 0, 10, 1, 0, 7, 4, 0, 1, 12, 1, 1, 1, 0, 1, 15, 0] = multiSearchAllPositionsUTF8(materialize('ощзллчубоггцвжриуардрулащйпу'), ['цвжр', '', 'нмзкаиудзтиффззшзканжвулт', 'лчубоггцвжриуардрулащйпу', 'чтцлзшуижолибаоххвшихбфжйхетивп', 'ггцвжри', '', 'йдгнвс', 'у', 'л', 'зпщнжуойдлдвхокцжнзйсйзе', '', 'цв', '', '', '', 'ехлцзгвф', '', 'риу', 'уйжгтжноомонгщ']) from system.numbers limit 10;
select [0, 12, 13, 20, 0, 1, 0, 0, 3, 4] = multiSearchAllPositionsUTF8(materialize('цбкифйтшузажопнжщарбштвдерзтдш'), ['щлмлижтншчсмксгтнсврро', 'жопнжщарбштвд', 'опнжщарб', 'бштвдерзтд', 'пуфслейщбкжмпнш', 'ц', 'маве', 'кмйхойрдлшцхишдтищвйбцкщуигваещгтнхйц', 'кифй', 'и']) from system.numbers limit 10;
select [0, 6, 0, 0, 0, 8, 0, 3, 6, 0] = multiSearchAllPositionsUTF8(materialize('еачачгбмомоххкгнвштггпчудл'), ['ндзчфчвжтцщпхщуккбеф', 'г', 'рткнфвчтфннхлжфцкгштймгмейжй', 'йчннбщфкщф', 'лсртщиндшшкичзррущвдйвнаркмешерв', 'момоххк', 'рфафчмсизлрхзуа', 'ч', 'гбмомоххкгнвштг', 'валжпошзбгзлвевчнтз']) from system.numbers limit 10;
select [0, 0, 10, 0, 8, 13, 0, 0, 19, 15, 3, 1] = multiSearchAllPositionsUTF8(materialize('зокимчгхухшкшмтшцчффвззкалпва'), ['цалфжажщщширнрвтпвмщжннрагвойм', 'оукзрдцсадешжмз', 'хшкшмтшцч', 'ауилтсаомуркпаркбцркугм', 'хухшкшмтшцчффв', 'шмтшцч', 'зщгшпцхзгцншднпеусмтжбцшч', 'щлраащсйлщрд', 'ффвзз', 'тшцчффвззкалпв', 'кимчгхухшкш', '']) from system.numbers limit 10;
select [0, 0, 1, 0, 6, 0, 6, 0, 5, 0, 13, 0, 0, 6] = multiSearchAllPositionsUTF8(materialize('йдйндиибщекгтчбфйдредпхв'), ['тдршвтцихцичощнцницшдхйбогбчубие', 'акппакуцйсхцдххнотлгирввоу', '', 'улщвзхохблтксчтб', 'и', 'ибейзчшклепзриж', 'иибщекгт', 'шидббеухчпшусцнрз', 'диибщекгтчбфйд', 'дейуонечзйзлдкшщрцйбйклччсцуй', 'тч', 'лшицлшме', 'чйнжчоейасмрщегтхвйвеевбма', 'ии']) from system.numbers limit 10;
select [15, 3, 3, 2, 0, 11, 0, 0, 0, 2, 0, 4, 0, 1, 1, 3, 0, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('нхгбфчшджсвхлкхфвтдтлж'), ['хфвтдтлж', 'гбфчшд', 'гбфчш', 'х', 'ачдгбккжра', 'вхлк', 'мщчвещлвшдщпдиимлшрвнщнфсзгщм', 'жчоббгшзщлгеепщжкчецумегпйчт', 'жжд', 'хг', 'мтсааролшгмоуйфйгщгтрв', 'бфчшд', 'чейрбтофпшишгуасоодлакчдф', 'н', 'нхгбфч', 'гбф', 'гдежсх', 'йифжацзгжбклх', 'ещпзущпбаолплвевфиаибшйубйцсзгт', 'жезгчжатзтучжб']) from system.numbers limit 10;
select [0, 10, 1, 0, 0, 0, 4, 0, 13, 1, 12, 1, 0, 6] = multiSearchAllPositionsUTF8(materialize('акбдестрдшерунпвойзв'), ['нркчх', 'шерунп', '', 'зжвахслфббтоиоцрзаззасгнфчх', 'шлжмдг', 'тлйайвцжчсфтцйрчосмижт', 'дестрдшерунп', 'мвамйшцбдщпчлрщд', 'у', 'акбдестрд', 'рунпвойз', '', 'айздцоилсйшцфнчтхбн', 'с']) from system.numbers limit 10;
select [1, 0, 0, 3, 2, 1, 0, 0, 1, 10, 7, 0, 5, 0, 8, 4, 1, 0, 8, 1] = multiSearchAllPositionsUTF8(materialize('кйхпукаеуддтйччхлнпсуклрф'), ['кйхпукаеуддтйччхл', 'йатлрйкстлхфхз', 'фгихслшкж', 'хпу', 'йхпукаеу', '', 'сруакбфоа', 'оажуз', 'кйхпукаеуддтйччх', 'ддтйччхлн', 'аеуддтйччхл', 'тмажиойщтпуцглхфишеиф', 'укаеуддтйччхлнпс', 'ретифе', 'еуддтйччхлнпсуклр', 'пукаеуд', 'кйхпу', 'таппфггвджлцпжшпишбпциуохсцх', 'еуд', '']) from system.numbers limit 10;
select [2, 3, 3, 16, 5, 13, 0, 0, 0, 18, 0, 6, 0, 16, 0, 10, 3, 0] = multiSearchAllPositionsUTF8(materialize('плврйщовкзнбзлбжнсатрцщщучтйач'), ['лврйщовкзнбзлбж', 'врйщовкзнбзлбжнса', 'врйщовкзнбз', 'жнсатрцщщучтйач', 'йщовкзнбзлбжнсатрцщщуч', 'злбжнсатрцщ', 'ввтбрдт', 'нжйапойг', 'ннцппгперхйвдхоеожупйебочуежбвб', 'сатрцщщу', 'деваийтна', 'щ', 'вкжйгкужжгтевлцм', 'жнс', 'датг', 'знбзлбжнсатрцщщучтйа', 'врйщовк', 'оашмкгчдзщефм']) from system.numbers limit 10;
select [3, 1, 19, 1, 0, 0, 0, 0, 11, 3, 0, 0] = multiSearchAllPositionsUTF8(materialize('фчдеахвщжхутхрккхасвсхепщ'), ['деах', '', 'свсхепщ', '', 'анчнсржйоарвтщмрж', 'нечбтшщвркгд', 'вштчцгшж', 'з', 'у', 'деахвщ', 'ххкцжрвзкжзжчугнфцшуиаклтмц', 'фцкжшо']) from system.numbers limit 10;
select [16, 0, 0, 1, 8, 14, 0, 12, 12, 5, 0, 0, 16, 0, 11, 0] = multiSearchAllPositionsUTF8(materialize('щмнжчввбжцчммчшсрхйшбктш'), ['срхйшбк', 'йлзцнржчууочвселцхоучмщфчмнфос', 'еижлафатшхщгшейххжтубзвшпгзмзцод', '', 'бжцчммчшсрхй', 'чшсрхй', 'влемчммйтителщвзган', 'ммч', 'ммчшсрх', 'чввбж', 'нобзжучшошмбщешлхжфгдхлпнгпопип', 'цгт', 'срхйш', 'лкклмйжтеа', 'чммчшсрхйшбктш', 'йежффзнфтнжхфедгбоахпг']) from system.numbers limit 10;
select [1, 12, 9, 5, 1, 0, 6, 3, 0, 1] = multiSearchAllPositionsUTF8(materialize('кжнщсашдзитдмщцхуоебтфжл'), ['', 'дмщцхуоебт', 'зитдмщцхуоебт', 'сашдзитдмщцхуое', 'кжнщ', 'тхкйтшебчигбтмглшеужззоббдилмдм', 'ашдзитдмщцхуоебтф', 'нщсашдз', 'аузщшр', 'кжнщсашдз']) from system.numbers limit 10;
select [2, 0, 0, 0, 1, 0, 2, 0, 0, 17, 0, 8, 7, 14, 0, 0, 0, 7, 9, 23] = multiSearchAllPositionsUTF8(materialize('закуфгхчтшивзчжаппбжнтслщввущ'), ['а', 'днойвхфрммтж', 'внтлжрхзрпчбтуркшдатннглечг', 'ахиеушжтфкгцщтзхмжнрхдшт', '', 'тцчгрззржмдшйщфдцрбшжеичч', 'а', 'ктиечцпршнфнбчуолипацчдсосцнлфаццм', 'аусрлхдцегферуо', 'ппбжнт', 'жкццуосгвп', 'чтшивзчжаппб', 'хчтшивзчжаппб', 'чжаппбжнтслщ', 'ччрлфдмлу', 'щзршффбфчзо', 'ущуймшддннрхзийлваежщухч', 'хчтши', 'тшивзчжаппбжнтсл', 'слщв']) from system.numbers limit 10;
select [1, 1, 9, 2, 0, 3, 7, 0, 0, 19, 2, 2, 0, 8] = multiSearchAllPositionsUTF8(materialize('мвкзккупнокченйнзкшбдрай'), ['м', '', 'н', 'вкз', 'гдпертшйбртотунур', 'к', 'упнокченйнзкшбдр', 'нфшрг', 'нмждрйббдцлйемжпулдвкещхтжч', 'ш', 'вкзккупнокченйнзкшбдр', 'вкзккупнокченйнзкшбдрай', 'адииксвеавогтйторчтцвемвойшпгбнз', 'пнокченй']) from system.numbers limit 10;
select [15, 0, 0, 1, 12, 1, 0, 0, 1, 11, 0, 4, 0, 2] = multiSearchAllPositionsUTF8(materialize('отарлшпсабждфалпшножид'), ['лпшно', 'вт', 'лпжшосндутхорлиифжаакш', 'отарлшпсабждфалпшнож', 'дфал', '', 'бкцжучншжбгзжхщпзхирртнбийбтж', 'уцвцкшдзревпршурбсвйнемоетчс', '', 'ждфал', 'тлскхрнпмойчбцпфущфгф', 'рлшпсабж', 'нхнмк', 'тарлшпса']) from system.numbers limit 10;
select [0, 2, 0, 20, 0, 17, 18, 0, 1, 1, 21, 1, 0, 1, 6, 26] = multiSearchAllPositionsUTF8(materialize('ачйвцштвобижнзжнчбппйеабтцнйн'), ['сзхшзпетншйисщкшрвйшжуогцвбл', 'чйвцштво', 'евз', 'пй', 'хуждапрахитйажрищуллйзвчт', 'чбппйе', 'бппйеабтцнйн', 'схш', 'а', 'ачйвцштвобижнзжнчбпп', 'йеабтцнй', '', 'ег', '', 'штвобижнзжнчбпп', 'цн']) from system.numbers limit 10;
select [1, 0, 0, 3, 4, 12, 0, 9, 0, 12, 0, 0, 8, 0, 10, 3, 4, 1, 1, 9] = multiSearchAllPositionsUTF8(materialize('жмхоужежйуфцзеусеоднчкечфмемба'), ['', 'идосйксзнщйервосогф', 'тхмсйлвкул', 'хоужежйуф', 'оужежйуфцзеусеоднчкечфм', 'цзеусеоднчкеч', 'бецвдиубххвхйкажуурщщшщфбзххт', 'йуфцзеусеодн', 'мглкфтуеайсржисстнпкгебфцпа', 'цзеусео', 'уехцфучецчгшйиржтсмгхакчшввохочжпухс', 'дчвмсбткзталшбу', 'жйуфцзеусеоднчке', 'ччшщтдбпвчд', 'уфцзеусеоднчкечфмем', 'хоужежйуфцзеусеоднчкечф', 'оуже', '', 'жмхоужежйуфцзеу', 'й']) from system.numbers limit 10;
select [0, 0, 0, 3, 0, 0, 0, 0, 1, 0, 1, 0, 1, 2, 0, 0, 0, 6] = multiSearchAllPositionsUTF8(materialize('лшпцхкмтресзпзйвцфрз'), ['енрнцепацлщлблкццжсч', 'ецжужлуфаееоггрчохпчн', 'зхзнгасхебнаейбддсфб', 'пцхкмтресзпзйв', 'фчетгеодщтавиииухцундпнхлчте', 'шшгсдошкфлгдвкурбуохзчзучбжйк', 'мцщщцп', 'рх', '', 'зйошвщцгхбж', '', 'ввлпнамуцвлпзеух', '', 'шпцхкмтре', 'маабтруздрфйпзшлсжшгож', 'фдчптишмштссщшдшгх', 'оллохфпкаем', 'кмтресзпз']) from system.numbers limit 10;
select [2, 5, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 1, 1, 12, 0, 0, 0, 4, 8] = multiSearchAllPositionsUTF8(materialize('есипзсвшемлхчзмйрсфз'), ['с', 'з', 'пщчсмаиахппферзжбпвиибаачй', 'гтщкзоиежав', 'свшемлхчзм', 'шийанбке', 'зхе', 'авркудфаусзквкфффйцпзжщввенттб', 'ножцваушапиж', 'иизкежлщиафицкчщмалнпсащсднкс', 'вчмв', 'кщеурмуужжлшррце', '', '', 'х', 'алзебзпчеложихашжвхмйхрицн', 'тпзмумчшдпицпдшиаог', 'сулксфчоштаййзбзшкджббщшсей', 'пзсвшемлхчзм', 'ш']) from system.numbers limit 10;
select [0, 1, 2, 4, 0, 0, 14, 1, 13, 4, 0, 0, 1, 1] = multiSearchAllPositionsUTF8(materialize('сзиимонзффичвфжоеулсадону'), ['зфтшебтршхддмеесчд', '', 'зиимонзф', 'имон', 'езбдйшжичценлгршщшаумайаицй', 'птпщемтбмднацлг', 'фжоеулса', '', 'вфжоеулсадону', 'имонзфф', 'йщвдфдиркважгйджгжашарчучйххйднпт', 'дй', '', '']) from system.numbers limit 10;
select [12, 0, 24, 0, 9, 0, 1, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('ижсщщрзжфнгццпзкфбвезгбохлж'), ['ццпзкфбвез', 'ацррвхоптаоснулнжкщжел', 'охлж', 'тнсхбпшщнб', 'фнг', 'урйвг', '', 'цохс', 'щбйрйкжчмйзачуефч', 'афа']) from system.numbers limit 10;
select [9, 0, 0, 0, 1, 0, 7, 7, 0, 0, 1, 0, 7, 0, 0, 8, 0, 3, 0, 0] = multiSearchAllPositionsUTF8(materialize('рерфвирачйнашхрмцебфдйааеммд'), ['чйнашхрмцебфдйааеммд', 'сжщзснвкущлжплцзлизаомдизцнжлмййбохрцч', 'еппбжджмримфчйеаолидпцруоовх', 'едтжкоийггснехшсчйлвфбкцжжрчтш', '', 'пжахфднхсотй', 'ра', 'рач', 'вчримуцнхбкуйжрвфиугзфсзг', 'кщфехрххциаашщсифвашгйцвхевцщнйахтбпжщ', '', 'ртщиобчжстовйчфабалзц', 'рачйнашхрмцебфдйаае', 'ощгжосччфкуг', 'гехвжнщжссидмрфчйтнепдсртбажм', 'а', 'ицжлсрсиатевбвнжрдмзцувввтзцфтвгвш', 'рф', 'прсмлча', 'ндлхшцааурмзфгверуфниац']) from system.numbers limit 10;
select [2, 14, 10, 0, 6, 15, 1, 0, 0, 4, 5, 17, 0, 0, 3, 0, 3, 0, 9, 0] = multiSearchAllPositionsUTF8(materialize('влфощсшкщумчллфшшвбшинфппкчуи'), ['лфощ', 'лфшшвбшинфпп', 'умчллфшшвбшинф', 'слмтнг', 'сшкщумчллфшшвбшинф', 'фшшвб', '', 'рчфбчййсффнодцтнтнбцмолф', 'щфнщокхжккшкудлцжрлжкнп', 'ощ', 'щсшкщумчлл', 'швбшинфппкч', 'септзкщотишсехийлоцчапщжшжсфмщхсацг', 'нт', 'фощсшкщумчллфшшвбшинфп', 'нщпдш', 'фощс', 'мивсмча', 'щумч', 'щчйнткжпмгавфтйтибпхх']) from system.numbers limit 10;
select [0, 10, 0, 0, 0, 0, 0, 3, 0, 0, 0, 2, 0, 11, 0, 0] = multiSearchAllPositionsUTF8(materialize('еаиалмзхцгфунфеагшчцд'), ['йнш', 'гфун', 'жлйудмхнсвфхсуедспщбтутс', 'елмуийгдйучшфлтхцппамфклйг', 'евйдецц', 'пчтфцоучфбсйщпвдацмчриуцжлтжк', 'нстмпумчспцвцмахб', 'иалмз', 'зифчп', 'чогфщимоопт', 'фдйблзеп', 'аиа', 'щугмзужзлйдктш', 'фунфеагшч', 'нйхшмсгцфжчхжвхгдхцуппдц', 'асмвмтнрейшгардллмсрзгзфйи']) from system.numbers limit 10;
select [23, 0, 8, 0, 0, 0, 0, 0, 0, 4, 0, 5, 7, 1, 9, 4] = multiSearchAllPositionsUTF8(materialize('зузйфзлхходфрхгтбпржшрктпйхеоп'), ['ктпйхео', 'лжитуддикчсмкглдфнзцроцбзтсугпвмхзллжж', 'х', 'меуфтено', 'фтдшбшрпоцедктсийка', 'кхтоомтбчвеонксабшйптаихжбтирпзшймчемжим', 'чиаущлрдкухцрдумсвивпафгмр', 'фрнпродв', 'тдгтишхйсашвмдгкчбмшн', 'йфзлхходфрхгтбпржшр', 'бежшлрйврзмумеуооплкицхлйажвцчнчсеакм', 'ф', 'лхходфрхгтб', '', 'ходфрхгтбпржшр', 'й']) from system.numbers limit 10;
select [0, 0, 0, 1, 0, 1, 22, 1, 0, 0, 0, 0, 18, 1, 0, 0, 0, 1] = multiSearchAllPositionsUTF8(materialize('чфгвчхчпщазтгмбнплдгщикойчднж'), ['мштцгтмблаезочкхзвхгрбпкбмзмтбе', 'канбжгсшхшз', 'кзинвщйччажацзйнсанкнщ', 'чфгвчхчпщазтгмбнп', 'етйцгтбнщзнржнйхж', '', 'ик', '', 'еизщвпрохдгхир', 'псумйгшфбвгщдмхжтц', 'слмжопинйхнштх', 'йшралцицммбщлквмгхцввизопнт', 'л', 'чфгвчхчпщазтгмбнплдгщ', 'пбзмхжнпгикиищжтшботкцеолчцгхпбвхи', 'хзкцгрмшгхпхуоцгоудойнжлсоййосссмрткцес', 'ажуофйпщратдйцбржжлжнжащцикжиа', '']) from system.numbers limit 10;
select [6, 0, 2, 5, 2, 9, 10, 0, 0, 4, 0, 6, 3, 2] = multiSearchAllPositionsUTF8(materialize('ишогпсисжашфшлйичлба'), ['сисжашфшлй', 'пднещбгзпмшепкфосовбеге', 'шогп', 'пс', 'шогпси', 'жаш', 'аш', 'деисмжатуклдшфлщчубфс', 'грмквкщзур', 'гпсис', 'кйпкбцмисчхдмшбу', 'сисжашф', 'о', 'шо']) from system.numbers limit 10;
select [8, 15, 13, 0, 1, 2, 5, 2, 9, 0, 0, 0] = multiSearchAllPositionsUTF8(materialize('нсчщчвсанпрлисблснокзагансхм'), ['анпрлисблснокзагансхм', 'блснокз', 'исб', 'дрмгвснпл', '', 'счщчвса', 'чвсанпрлисблснокзагансх', 'счщчвсанпрлис', 'нпрли', 'пциишуецнймуодасмжсойглретиефо', 'фхимщвкехшлг', 'слщмаимшжчфхзпрцмхшуниврлуйлжмфжц']) from system.numbers limit 10;
select [0, 5, 0, 0, 14, 0, 12, 0, 2, 3, 0, 3, 21, 5] = multiSearchAllPositionsUTF8(materialize('хажуижанндвблищдтлорпзчфзк'), ['щуфхл', 'и', 'фцежлакчннуувпаму', 'щесщжрчиктфсмтжнхекзфс', 'ищдтлорпзчф', 'дееичч', 'блищ', 'гиефгйзбдвишхбкбнфпкддмбтзиутч', 'ажуижа', 'жуижанндвблищдтлорпзчфзк', 'чщщдзетвщтччмудвзчгг', 'ж', 'пзчфз', 'ижанн']) from system.numbers limit 10;
select [0, 0, 0, 9, 15, 0, 0, 0, 1, 3, 0, 0, 1, 0, 10, 0, 4, 0, 0, 7] = multiSearchAllPositionsUTF8(materialize('россроапцмцагвиигнозхзчотус'), ['ошажбчвхсншсвйршсашкм', 'пфдчпдчдмауцгкйдажрйефапввшжлшгд', 'иеаочутввжмемчушлуч', 'цмцагвиигно', 'ииг', 'ммпжщожфйкакбущчирзоммагеиучнщмтвгихк', 'укррхбпезбжууеипрзжсло', 'ншопзжфзббилйбувгпшшиохврнфчч', '', 'ссроап', 'лийщфшдн', 'йчкбцциснгначдцйчпа', 'россроапцмцагвииг', 'кштндцтсшорввжсфщчмщчжфжквзралнивчзт', 'мца', 'нбтзетфтздцао', 'сроа', 'мщсфие', 'дткодбошенищйтрподублжскенлдик', 'апцмцагвиигноз']) from system.numbers limit 10;
select [16, 0, 0, 2, 1, 1, 0, 1, 9, 0, 0, 3] = multiSearchAllPositionsUTF8(materialize('тйсдйилфзчфплсджбарйиолцус'), ['жбарйиолцу', 'цназщжждефлбрджктеглщпунйжддгпммк', 'хгжоашцшсзкеазуцесудифчнощр', 'йс', '', 'тйсдйилфзчфп', 'ивфсплшвслфмлтххжчсстзл', '', 'зчфплсдж', 'йртопзлодбехрфижчдцйс', 'цлащцкенмшеоерееиуноп', 'с']) from system.numbers limit 10;
select [3, 2, 1, 1, 0, 0, 0, 14, 6, 0] = multiSearchAllPositionsUTF8(materialize('нсцннйрмщфбшщховвццбдеишиохл'), ['цннйр', 'сцннйрм', 'н', 'нс', 'двтфхйзгеиеиауимбчхмщрцутф', 'пчтмшйцзсфщзшгнхщсутфжтлпаввфгххв', 'лшмусе', 'ховвццбд', 'йрмщфбшщховвццбдеи', 'гндруущрфзсфжикшзцжбил']) from system.numbers limit 10;
select [0, 18, 0, 1, 2, 0, 0, 0, 1, 7, 10, 0, 1, 0, 2, 0, 0, 18] = multiSearchAllPositionsUTF8(materialize('щидмфрсготсгхбомлмущлаф'), ['тлтфхпмфдлуоцгчскусфжчкфцхдухм', 'мущла', 'емлвзузхгндгафги', '', 'идмфрсготсгхбомлмущла', 'зфаргзлщолисцфдщсеайапибд', 'кдхоорхзжтсйимкггйлжни', 'лчгупсзждплаблаеклсвчвгвдмхклщк', 'щидмфр', 'сготсгхбомлму', 'тсгхбомлмущла', 'хсзафйлкчлди', '', 'й', 'ид', 'щлйпмздйхфзайсщсасейлфцгхфк', 'шдщчбшжбмййзеормнрноейй', 'мущ']) from system.numbers limit 10;
select [0, 13, 0, 0, 1, 0, 7, 7, 8, 0, 2, 0, 3, 0, 0, 13] = multiSearchAllPositionsUTF8(materialize('трцмлщввадлжввзчфипп'), ['хшзйийфжмдпуигсбтглй', 'ввзчфи', 'нсцчцгзегммтсшбатщзузпкшрг', 'гувйддежзфилйтш', '', 'хгзечиа', 'ввадлжввз', 'ввадлжввзчфи', 'ва', 'щтшсамклегш', 'рцмлщ', 'учзмиерфбтцучйдглбщсз', 'цмлщввадлжввзчфи', 'орйжччцнаррбоабцжзйлл', 'квпжматпцсхзузхвмйч', 'ввзчфип']) from system.numbers limit 10;
select [0, 1, 1, 0, 11, 4, 1, 2, 0, 0] = multiSearchAllPositionsUTF8(materialize('инкщблбвнскцдндбмсщщш'), ['жхрбсусахрфкафоилмецчебржкписуз', 'инкщблбвнс', '', 'зисгжфлашймлджинаоджруй', 'кцднд', 'щблбвнскцдндбмсщщ', 'инкщблбвнс', 'н', 'зб', 'фчпупшйфшбдфенгитатхч']) from system.numbers limit 10;
select [6, 0, 4, 20, 1, 0, 5, 0, 1, 0] = multiSearchAllPositionsUTF8(materialize('рзтецуйхлоорйхдбжашнларнцт'), ['у', 'бпгййекцчглпдвсцсещщкакцзтцбччввл', 'ецуйхлоо', 'нлар', 'рз', 'ккнжзшекфирфгсгбрнвжчл', 'цуйхлоорйхдбжашн', 'йнучгрчдлйвводт', 'рзте', 'нткрввтубчлщк']) from system.numbers limit 10;

select [1, 1, 0, 0, 1, 0, 0, 3, 3, 3, 1, 0, 8, 0, 8, 1, 0, 1] = multiSearchAllPositionsCaseInsensitive(materialize('OTMMDcziXMLglehgkklbcGeAZkkdh'), ['', 'OTmmDCZiX', 'SfwUmhcGTvdYgxlzsBJpikOxVrg', 'ngqLQNIkqwguAHyqA', '', 'VVZPhzGizPnKJAkRPbosoNGJTeO', 'YHpLYTVkHnhTxMODfABor', 'mMdcZi', 'MmdCZI', 'MMdCZixmlg', '', 'hgaQHHHkIQRpPjv', 'ixMLgLeHgkkL', 'uKozJxZBorYWjrx', 'i', '', 'WSOYdEKatHkWiCtlwsCbKRnXuKcLggbkBxoq', '']) from system.numbers limit 10;
select [4, 15, 0, 0, 0, 0, 5, 0, 5, 1, 0, 1, 13, 0, 0, 3] = multiSearchAllPositionsCaseInsensitive(materialize('VcrBhHvWSFXnSEdYCYpU'), ['bhhVwSfXnSEd', 'DycyP', 'kEbKocUxLxmIAFQDiUNoAmJd', 'bsOjljbyCEcedqL', 'uJZxIXwICFBPDlUPRyDHMmTxv', 'BCIPfyArrdtv', 'hHv', 'eEMkLteHsuwsxkJKG', 'hHVWsFxNseDy', '', 'HsFlleAQfyVVCoOSLQqTNTaA', '', 'sEDY', 'UMCKQJY', 'j', 'rBhHvw']) from system.numbers limit 10;
select [1, 1, 0, 0, 1, 0, 0, 0, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('wZyCLyiWnNNdNAPWeGSQZcdqk'), ['w', '', 'vlgiXgFTplwqRbnwBumAjHvQuM', 'QoIRVKDHMlapLNiIZXvwYxluUivjY', 'WZY', 'gAFpUfPDAwgzARCIMrtbZUsNcR', 'egkLWqqdNiETeETsMG', 'dzSlJaoHKlQmENIboow', 'vPNBhcaIfsgLH', 'mlWPTCBDVTdKHxlvIUVcJXBrmTcJokAls']) from system.numbers limit 10;
select [0, 10, 0, 1, 7, 1, 6, 1, 8, 0] = multiSearchAllPositionsCaseInsensitive(materialize('pqliUxqpRcOOKMjtrZSEsdW'), ['YhskuppNFdWaTaZo', 'Coo', 'mTEADzHXPeSMCQaYbKpikXBqcfIGKs', 'PQLiUxq', 'qpRCoOK', 'PQLIu', 'XQPrcoOK', '', 'pR', 'cTmgRtcSdRIklNQVcGZthwfarLtAYh']) from system.numbers limit 10;
select [16, 1, 1, 1, 1, 4, 17, 0, 0, 0, 1, 0, 0, 0, 20, 0] = multiSearchAllPositionsCaseInsensitive(materialize('kJyseeDFCeUWoqMfubYqJqWA'), ['fub', 'kJY', '', '', 'Kj', 's', 'uBYQJq', 'sUqCmHUZIBtZPswObXSrYCwrdxdznM', 'mtZDCJENYuikJnCcJfRcSCDYDPXU', 'IDXjRjHhmjqXmCOlQ', '', 'jiEwAxIsJDu', 'YXqcEKbHxlgUliIALorSKDMlGGWeCO', 'OstKrLpYuASEUrIlIuHIRdwLr', 'qJq', 'tnmvMTFvjsW']) from system.numbers limit 10;
select [11, 3, 1, 0, 9, 0, 0, 0, 0, 8, 3, 0] = multiSearchAllPositionsCaseInsensitive(materialize('EBSPtFpDaCIydASuyreS'), ['iyD', 'sptfpdAciyDAsuyR', 'EbS', 'IJlqfAcPMTUsTFXkvmtsma', 'AcIYda', 'fbWuKoCaCpRMddUr', 'srlRzZKeOQGGLtTLOwylLNpVM', 'ZeIgfTFxUyNwDkbnpeiPxQumD', 'j', 'daciydA', 'sp', 'dyGFtyfnngIIbcCRQzphoqIgIMt']) from system.numbers limit 10;
select [6, 0, 0, 0, 10, 0, 1, 4, 0, 15, 0, 2, 2, 6] = multiSearchAllPositionsCaseInsensitive(materialize('QvlLEEsgpydemRZAZcYbqPZHx'), ['eSgpYDEMRzAzcyBQPzH', 'NUabuIKDlDxoPXoZOKbUMdioqwQjQAiArv', 'pRFrIAGTrggEOBBxFmnZKRPtsUHEMUEg', 'CDvyjef', 'YdEMrzaZc', 'BO', '', 'leEsgPyDEmRzaZCYBqPz', 'EzcTkEbqVXaVKXNuoxqNWHM', 'Z', 'cuuHNcHCcLGb', 'V', 'vllEes', 'eS']) from system.numbers limit 10;
select [0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 5, 7, 5, 0, 11, 1] = multiSearchAllPositionsCaseInsensitive(materialize('eiCZvPdGJSmwxMIrZvEzfYFOFJmV'), ['lSydrmJDeXDYHGFFiFOOJGyCbCCDbLzbSbub', 'ewsAVflvcTBQFtvWBwuZOJKkrUArIg', 'fpEkBWaBkRWypFWtMz', 'YatSURyNtcSuerWWlTBSdBNClO', 'YO', 'CZvpdg', 'uoH', 'gtGwQSVqSJDVROmsBIxjuVNfrQnxDhWGXLBH', 'IKNs', 'HElLuRMlsRgINaNp', 'V', 'DGjsMW', 'vPDgJSmW', 'SGCwNiAmNfHSwLGZkRYEqrxBTaDRAWcyHZYzn', 'mWXMiRZvezfYf', '']) from system.numbers limit 10;
select [23, 1, 0, 17, 0, 0, 9, 3, 0, 2] = multiSearchAllPositionsCaseInsensitive(materialize('BizUwoENfLxIIYVDflhOaxyPJw'), ['yPJ', '', 'gExRSJWtZwOptFTkNlBGuxyQrAu', 'FLH', 'hCqo', 'oVGcArersxMUCNewhTMmjpyZYAIU', 'FlXIiYVdflHoAX', 'ZuWOe', 'bhfAfNdgEAtGdHylxkjgvU', 'IZUWo']) from system.numbers limit 10;
select [0, 9, 0, 0, 0, 0, 1, 0, 0, 1, 3, 0, 13, 0, 3, 5] = multiSearchAllPositionsCaseInsensitive(materialize('loKxfFSIAjbRcguvSnCdTdyk'), ['UWLIDIermdFaQVqEsdpPpAJ', 'ajBrcg', 'xmDmuYoRpGu', 'wlNjlKhVzpC', 'MxIjTspHAQCDbGrIdepFmLHgQzfO', 'FybQUvFFJwMxpVQRrsKSNHfKyyf', '', 'vBWzlOChNgEf', 'DiCssjczvdDYZVXdCfdSDrWaxmgpPXDiD', '', 'kxFFSIAjBRCGUVSNcD', 'LrPRUqeehMZapsyNJdu', 'cGuVSNcdTdy', 'NmZpHGkBIHVSoOcj', 'KxffSIAjBr', 'ffsIaJB']) from system.numbers limit 10;
select [14, 0, 11, 0, 10, 0, 0, 0, 13, 1, 2, 11, 5, 0] = multiSearchAllPositionsCaseInsensitive(materialize('uijOrdZfWXamCseueEbq'), ['sE', 'VV', 'AmcsEu', 'fUNjxmUKgnDLHbbezdTOzyLaknQ', 'XAmCsE', 'HqprIpxIcOTkDIKcVK', 'NbmirQlNsTHnAVKlF', 'VVDNOxFKSnQGKPsTqgtwLhZnIPkL', 'c', '', 'IJ', 'aM', 'rDzF', 'YFwP']) from system.numbers limit 10;
select [0, 8, 17, 0, 1, 0, 0, 0, 0, 0, 5, 0] = multiSearchAllPositionsCaseInsensitive(materialize('PzIxktujxHZsaDlwSGQPgvA'), ['zrYlZdnUxlPrVJJeZEASwdCHlNEm', 'jxhZS', 'sGQPgV', 'MZMChmRBgsxhdgspUhALoxmrkZVp', 'pzIxktuJxHzsADlw', 'xavwOAibQuoKg', 'vuuETOrWLBNLhrMeWLgGQpeFPdcWmWu', 'TZrAgmdorqZIdudhyCMypHYKFO', 'ztcCyGxRKrcUTv', 'OUvwdMZrcZuwGtjuEBeGU', 'k', 'rFTpnfGIOCfwktWnyOMeXQZelkYwqZ']) from system.numbers limit 10;
select [3, 1, 4, 1, 0, 17, 13, 0, 0, 0, 0, 0, 8, 0] = multiSearchAllPositionsCaseInsensitive(materialize('pUOaQLUvgmqvxaMsfJpud'), ['OaqLUvGm', '', 'aQ', '', 'VajqJSlkmQTOYcedjiwZwqNH', 'f', 'xaMsfj', 'CirvGMezpiIoacBGAGQhTJyr', 'vucKngiFjTlzltKHexFVFuUlVbey', 'ppalHtIYycBCEjsgsXbFeecpkQMNr', 'nEgIYVoGkhTsFgBUSHJvIcYCYbuOBP', 'efjBVRVzknGrikGHxExlFEtYf', 'v', 'QgRBCaGlwNYWRslDylOrfPxZxAOF']) from system.numbers limit 10;
select [14, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1, 0, 20, 5, 0, 4, 0] = multiSearchAllPositionsCaseInsensitive(materialize('WZNWOCjFkCAAzIptkUtyPCyC'), ['iPTkuT', 'BngeNlFbKymzMYmNPfV', 'XKEjbLtADFMqS', 'dbRQKJGSFhzljAiZV', 'wZnwoCjFKCAAzIPTKuTYpc', 'yBaUvSSGOEL', 'iEYopROOYKxBwPdCgbPNPAsMwVksHgagnO', 'TljXPJVebHqrnhSiTGwpMaNeKy', 'wzNWocjF', 'bLxLrZnOCeIfxkfZEOcqDteUvc', 'CtHYpAZDANEv', '', 'XMAMpGYMiOb', 'y', 'o', 'floswnnFjXDTxantSvDYPSnaORL', 'WOcjFkcAaZIp', 'buqBHbZsLDnCUDhLdgd']) from system.numbers limit 10;
select [0, 20, 14, 0, 2, 0, 1, 14, 0, 0, 0, 1, 0, 26, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('XJMggEHaxfddDadtwKMCcPsMlSFVJ'), ['NzbUAZvCsnRnuzTglTsoT', 'ccP', 'ADTwKmc', 'JaUzcvWHMotuEMUtjsTfJzrsXqKf', 'jMGgEHaXfdddAdTWKMCcpsM', 'SMnb', '', 'AdTWkMccPSMlsfv', 'fVjPVafkp', 'goqsYAFqhhnCkGwhg', 'CNHNPZHZreFwhRMr', '', 'vcimNhmdbtoiCgVzNuvdgZG', 'sfvJ', 'AqKmroxmRMSFAKjfhwrzxmNSSjMHxKow', 'Xhub']) from system.numbers limit 10;
select [0, 0, 7, 0, 1, 1, 0, 0, 13, 0, 1, 1, 5, 0] = multiSearchAllPositionsCaseInsensitive(materialize('VQuEWycGbGcTcCCvWkujgdoWjKgVYy'), ['UevGaXmEAtBdWsPhBfqp', 'aQOrNMPmoVGSu', 'c', 'TMhzvbNJCaxtGNUgRBmTFEqgNBIBpSJ', '', 'vq', 'pVNUTCqXr', 'QSvkansbdPbvVmQpcQXDk', 'cCCvwkUjgdOWjKgVYy', 'EtCGaEzsSbJ', 'V', '', 'WycgBgCTCcCvwkujgdoWJKgv', 'xPBJqKrZbZHJawYvPxgqrgxPN']) from system.numbers limit 10;
select [4, 1, 0, 0, 0, 0, 0, 0, 0, 18] = multiSearchAllPositionsCaseInsensitive(materialize('LODBfQsqxfeNuoGtzvrUMRVWNKUKKs'), ['Bf', 'lOdbfQs', 'ZDSDfKXABsFiZRwsebyU', 'DT', 'GEUukPEwWZ', 'GNSbrGYqEDWNNCFRYokZbZEzGzc', 'kYCF', 'Kh', 'jRMxqdmGYpTkePeReXJNdnxagceitMJlmbbro', 'VrumrvWnKU']) from system.numbers limit 10;
select [1, 1, 3, 1, 10, 0, 9, 2, 2, 0, 0, 0, 0, 0, 8, 0, 1, 11, 8, 0] = multiSearchAllPositionsCaseInsensitive(materialize('lStPVtsQypFlZQoQhCuP'), ['', '', 'tpV', 'L', 'PF', 'pGPggwbkQMZandXugTpUorlPOubk', 'yPFlz', 'sTPVTsQyPfLzQOqhCU', 'StPVtSq', 'cbCxBjAfJXYgueqMFNIoSguFm', 'AosIZKMPduRfumDZ', 'AGcNTHObH', 'oPaGpsQ', 'kwQCczyY', 'q', 'HHUYdzGAzVJyn', '', 'fLZQoqHcUp', 'q', 'SSonzfqLVwIGzdHtj']) from system.numbers limit 10;
select [0, 1, 2, 0, 0, 0, 13, 1, 27, 1, 0, 1, 3, 1, 0, 1, 3, 0] = multiSearchAllPositionsCaseInsensitive(materialize('NhKJtvBUddKWpseWwRiMyBsTWmlk'), ['toBjODDZoRAjFeppAdsne', '', 'HKjTvBu', 'QpFOZJzUHHQAExAqkdoBpSbXzPnTzuPd', 'gE', 'hLmXhcEOwCkatUrLGuEIJRkjATPlqBjKPOV', 'Ps', 'NH', 'l', '', 'aSZiWpmNKfglqAbMZpEwZKmIVNjyJTtDianY', 'NhKJTvBUDDkwpS', 'KJtvbUDDKWPSewwrimYbstwm', 'NHKJTvbudDKwpSEwwR', 'hmMeWEpksVAaXd', 'NHkJTvBUDd', 'kjTvbudd', 'kmwUzfEpWSIWkEylDeRPpJDGb']) from system.numbers limit 10;
select [0, 5, 0, 0, 0, 1, 1, 15, 2, 3, 4, 5] = multiSearchAllPositionsCaseInsensitive(materialize('NAfMyPcNINKcgsShJMascJunjJva'), ['ftHhHaJoHcALmFYVvNaazowvQlgxwqdTBkIF', 'yp', 'zDEdjPPkAdtkBqgLpBfCtsepRZScuQKbyxeYP', 'yPPTvdFcwNsUSeqdAUGySOGVIhxsJhMkZRGI', 'JQEqJOlnSSam', 'nAFmy', '', 'sHJmaScjUnJj', 'afmY', 'FmYpcnINKCg', 'MYPCniNkcgSS', 'YPCNiNkCgSsHjmasCJuNjJ']) from system.numbers limit 10;
select [0, 0, 6, 3, 2, 0, 8, 2, 2, 10, 0, 0, 14, 0, 0, 3] = multiSearchAllPositionsCaseInsensitive(materialize('hgpZVERvggiLOpjMJhgUhpBKaN'), ['Nr', 'jMcd', 'e', 'PZVeRvggiLOPjmjh', 'GpZVe', 'cVbWQeTQGhYcWEANtAiihYzVGUoHKH', 'VGgilOPj', 'GPZVervgGiLopjmjHGuHp', 'GP', 'gil', 'fzwDPTewvwuCvpxNZDi', 'gLLycXDitSXUZTgwyeQgMSyC', 'PJmjh', 'bTQdrFiMiBtYBcEnYbKlqpTvGLmo', 'ggHxiDatVcGTiMogkIWDxmNnKyVDJth', 'pzv']) from system.numbers limit 10;
select [7, 1, 9, 3, 0, 0, 2, 0, 1, 11] = multiSearchAllPositionsCaseInsensitive(materialize('xUHVawrEvgeYyUZGmGZejClfinvNS'), ['RevGeYyuz', 'XUHvAWrev', 'Vg', 'hvawR', 'eRQbWyincvqjohEcYHMwmDbjU', 'nuQCxaoxEdadhptAhZMxkZl', 'UhVAwREvGEy', 'lHtwTFqlcQcoOAkujHSaj', '', 'eYYUzgMgzEjCLfIn']) from system.numbers limit 10;
select [0, 0, 8, 5, 9, 1, 0, 4, 12, 6, 4, 0, 0, 12] = multiSearchAllPositionsCaseInsensitive(materialize('DbtStWzfvScJMGVPQEGkGFoS'), ['CSjYiEgihaqQDxZsOiSDCWXPrBdiVg', 'aQukOYRCSLiildgifpuUXvepbXuAXnYMyk', 'fvsCjmgv', 'TWZFV', 'VscjMgVpQ', 'dBtSTwZfVsCjmGVP', 'wqpMklzJiEvqRFnZYMfd', 'StwZfVScJ', 'j', 'wzfVsCjmGV', 'STWZfVS', 'kdrDcqSnKFvKGAcsjcAPEwUUGWxh', 'UtrcmrgonvUlLnzWXvZI', 'jMgvP']) from system.numbers limit 10;
select [0, 0, 0, 0, 7, 3, 0, 11, 1, 10, 0, 0, 7, 1, 4, 0, 17, 3, 15, 0] = multiSearchAllPositionsCaseInsensitive(materialize('YSBdcQkWhYJMtqdEXFoLfDmSFeQrf'), ['TnclcrBJjLBtkdVtecaZQTUZjkXBC', 'SPwzygXYMrxKzdmBRTbppBQSvDADMUIWSEpVI', 'QnMXyFwUouXBoCGLtbBPDSxyaLTcjLcf', 'dOwcYyLWtJEhlXxiQLRYQBcU', 'KWhYjMtqdEXFo', 'BD', 'nnPsgvdYUIhjaMRVcbpPGWOgVjJxoUsliZi', 'j', '', 'YjmtQdeXF', 'peeOAjH', 'agVscUvPQNDwxyFfXpuUVPJZOjpSBv', 'kWh', '', 'dcQKWHYjmTQD', 'qjWSZOgiTCJyEvXYqaPFqbwvrwadJsGVTOhD', 'xfoL', 'b', 'DeXf', 'HyBR']) from system.numbers limit 10;
select [4, 0, 0, 13, 1, 0, 3, 13, 16, 1, 0, 1, 16, 1, 12, 0, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('SoVPMQNqmaTGuzYxDvZvapSuPiaP'), ['pMqNQMAtGuzYxDVz', 'TEJtgLhyredMnIpoZfmWvNwpkxnm', 'XRWmsfWVOCHhk', 'u', '', 'HvkXtxFdhVIyccpzFFSL', 'VPM', 'uZyXDVzvAPsUpIaP', 'xDvzV', 'sovpmqNQmATguZYx', 'wEG', 'soVPmQnQ', 'XDVzV', '', 'GUZyXdvzva', 'FetUahWwGtwEpVdlJCJntL', 'B', 'lSCUttZM']) from system.numbers limit 10;
select [1, 0, 1, 2, 15, 0, 0, 0, 1, 0] = multiSearchAllPositionsCaseInsensitive(materialize('zFWmqRMtsDjSeWBSFoqvWsrV'), ['', 'GItrPyYRBwNUqwSaUBpbHJ', '', 'f', 'BsfOQvWsR', 'JgvsMUZzWaddD', 'wxRECkgoCBPjSMRorZpBwuOQL', 'xHKLLxUoWexAM', '', 'YlckoSedfStmFOumjm']) from system.numbers limit 10;
select [11, 1, 1, 1, 0, 0, 1, 0, 4, 0, 0, 0, 1, 0, 5, 8] = multiSearchAllPositionsCaseInsensitive(materialize('THBuPkHbMokPQgchYfBFFXme'), ['KpqGchyfBF', '', '', 'TH', 'NjnC', 'ssbzgYTybNDbtuwJnvCCM', 'tHbupKHBMOkPQgcHy', 'RpOBhT', 'uPKHbMoKpq', 'oNQLkpSKwocBuPglKvciSjttK', 'TaCqLisKvOjznOxnTuZe', 'HmQJhFyZrcfeWbXVXsnqpcgRlg', 'tHB', 'gkFGbYje', 'pkhbMokPq', 'Bm']) from system.numbers limit 10;
select [7, 10, 0, 0, 9, 0, 0, 3, 0, 10] = multiSearchAllPositionsCaseInsensitive(materialize('ESKeuHuVsDbiNtvxUrfPFjxblv'), ['uvsDBiNtV', 'DbInTvxu', 'YcLzbvwQghvrCtCGTWVuosE', 'cGMNo', 'SDb', 'nFIRTLImfrLpxsVFMBJKHBKdSeBy', 'EUSiPjqCXVOFOJkGnKYdrpuxzlbKizCURgQ', 'KeUHU', 'gStFdxQlrDcUEbOlhLjdtQlddJ', 'DBInTVx']) from system.numbers limit 10;
select [1, 0, 2, 18, 1, 3, 15, 8, 0, 0, 1, 3, 0, 23, 2, 0, 8, 0] = multiSearchAllPositionsCaseInsensitive(materialize('TzczIDSFtrkjCmDQyHxSlvYTNVKjMT'), ['', 'AmIFsYdYFaIYObkyiXtxgvnwMVZxLNlmytkSqAyb', 'ZcZI', 'HXsLVYTnvKjm', '', 'CZiDsFtRKJ', 'DQYhxSl', 'fTRKjCmdqYHxsLvYtNvk', 'hxVpKFQojYDnGjPaTNPhGkRFzkNhnMUeDLKnd', 'RBVNIxIvzjGYmQBNFhubBMOMvInMQMqXQnjnzyw', '', 'c', 'vcvyskDmNYOobeNSfmlWcpfpXHfdAdgZNXzNm', 'ytnvKJM', 'ZcZidsFtRKjcmdqy', 'IRNETsfz', 'fTR', 'POwVxuBifnvZmtBICqOWhbOmrcU']) from system.numbers limit 10;
select [14, 16, 10, 2, 6, 1, 0, 8, 0, 0, 12, 1, 0, 1, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('tejdZOLhjpFLkGBWTGPfmk'), ['GBWtgPF', 'Wt', 'PflkgBWTgpFmK', 'ejdZOLhJPFlKgb', 'o', 'TejDZ', 'HlQfCP', 'hJP', 'ydiyWEfPGyRwcKGfGVdYxAXmkY', 'QsOyrgkTGMpVUAmLjtnWEIW', 'LKGBw', 'tejDzolHJpFLKgbWT', 'IK', '', 'WrzLpcmudcIJEBapkToDbYSazKTwilW', 'DmEWOxoieDsQHYsLNelMc']) from system.numbers limit 10;
select [9, 0, 1, 4, 13, 0, 0, 1, 3, 7, 9, 0, 1, 1, 0, 7] = multiSearchAllPositionsCaseInsensitive(materialize('ZWHpzwUiXxltWPAIGGxIcJB'), ['XxLTWpA', 'YOv', '', 'pzwUIXXl', 'wp', 'lpMMLDAuflLnWMFrETXRethzCUZOWfQ', 'la', '', 'HPZ', 'UixxlTw', 'xXLTWP', 'YlfpbSBqkbddrVwTEmXxgymedH', '', '', 'QZWlplahlCRTMjmNBeoSlcBoKBTnNZAS', 'UiXxlTwPAiGG']) from system.numbers limit 10;
select [0, 9, 6, 0, 4, 0, 3, 0, 0, 0, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('NytxaLUvmiojEepjuCzwUYPoWL'), ['LcOnnmjbZSifx', 'm', 'lUvMIOjeE', 'vuZsNMSsutiLCDbClPUSsrziohmoZaQeXtKG', 'XaLuvm', 'hlUevDfTSEGOjvLNdRTYjJQvMvwrMpwy', 'TXALuVmioJeePjUczw', 'pKaQKZg', 'PAdX', 'FKLMfNAwNqeZeWplTLjd', 'DODpbzUmMCzfGZwfkjH', 'HMcEGRHLspYdJIiJXqwjDUBp']) from system.numbers limit 10;
select [2, 1, 0, 16, 8, 1, 6, 0, 0, 1, 8, 0, 7, 0, 9, 1, 1, 0, 0, 1] = multiSearchAllPositionsCaseInsensitive(materialize('WGVvkXuhsbzkLqiIEOuyiRfomy'), ['GVv', '', 'VbldWXHWzdziNcJKqIkDWrO', 'iEOUyIRFomy', 'hsBZklqiieOuy', '', 'X', 'emXjmIqLvXsNz', 'rxhVkujX', 'wgvvK', 'HsBzKLQiie', 'wVzJBMSdKOqjiNrXrfLEjjXozolCgYv', 'UHsbzklQiiEouyirf', 'UOvUsiKtUnwIt', 'SBZKLqiIEoUYIrfom', 'wg', '', 'BefhETEirL', 'WyTCSmbKLbkQ', '']) from system.numbers limit 10;
select [8, 1, 2, 8, 1, 0, 5, 0, 0, 4, 0, 1, 14, 0, 0, 7, 0, 1] = multiSearchAllPositionsCaseInsensitive(materialize('uyWhVSwxUFitYoVQqUaCVlsZN'), ['XufitYOVqqUACVlszn', '', 'ywH', 'XUFIT', 'uywHvSWXuFIt', 'dGhpjGRnQlrZhzGeInmOj', 'vswXuFitYovqQuA', 'dHCfJRAAQJUZeMJNXLqrqYCygdozjAC', 'rojpIwYfNLECl', 'hVswxufiTYov', 'bgJdgRoye', '', 'ovQ', 'AdVrJlq', 'krJFOKilvBTGZ', 'WxuFITYOV', 'AsskQjNPViwyTF', 'u']) from system.numbers limit 10;
select [0, 2, 0, 0, 0, 6, 0, 5, 0, 15, 0, 0, 3, 0] = multiSearchAllPositionsCaseInsensitive(materialize('BEKRRKLkptaZQvBxKoBL'), ['HTwmOxzMykTOkDVKjSbOqaAbg', 'eKrRKl', 'UrLKPVVwK', 'TyuqYmTlQDMXJUfbiTCr', 'fyHrUaoMGdq', 'KLkPtaZq', 'cPUJp', 'RKLk', 'yMnNgUOpDdP', 'BX', 'tXZScAuxcwYEfSKXzyfioYPWsrpuZz', 'dsiqhlAKbCXkyTjBbXGxOENd', 'k', 'juPjORNFlAoEeMAUVH']) from system.numbers limit 10;
select [9, 0, 0, 0, 1, 4, 2, 0, 0, 0, 0, 8, 0, 2, 0, 3, 0, 3] = multiSearchAllPositionsCaseInsensitive(materialize('PFkLcrbouhBTisTkuUcO'), ['UhBtistKU', 'ioQunYMFWHD', 'VgYHTKZazRtfgRtvywtIgVoBqNBwVn', 'ijSNLKch', 'pFKlcrBOuhbtIsTku', 'lCRboUHBtI', 'fKLCRBOu', 'XTeBYUCBQVFwqRkElrvDOpZiZYmh', 'KzXfBUupnT', 'OgIjgQO', 'icmYVdmekJlUGSmPLXHc', 'OuH', 'BWDGzBZFhTKQErIRCbtUDIIjzw', 'F', 'LuWyPfSdNHIAOYwRMFhP', 'kL', 'PQmvXDCkEhrlFBkUmRqqWBxYi', 'kLcrbo']) from system.numbers limit 10;
select [0, 1, 1, 6, 14, 3, 0, 1, 9, 1, 9, 0, 1, 10, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('pfynpJvgIjSqXWlZzqSGPTTW'), ['ZzeqsJPmHmpoYyTnKcWJGReOSUCITAX', '', 'P', 'jvGIj', 'wLZzQsgP', 'YnPjVGij', 'DmpcmWsyilwHwAFcKpLhkiV', '', 'I', 'pFy', 'IjsqxwLZzqSgpT', 'pKpe', 'PfynpJvgiJSqXwlzZ', 'jsQXwLZZqs', 'onQyQzglEOJwMCO', 'GV']) from system.numbers limit 10;
select [1, 17, 1, 20, 0, 0, 5, 0, 0, 0, 24, 0] = multiSearchAllPositionsCaseInsensitive(materialize('BLNRADHLMQstZkAlKJVylmBUDHqEVa'), ['bLnRaDhLm', 'kJVYlmbuD', 'bLnr', 'yLMbU', 'eAZtcqAMoqPEgwtcrHTgooQcOOCmn', 'jPmVwqZfp', 'aDHlmqS', 'fmaauDbUAQsTeijxJFhpRFjkbYPX', 'aqIXStybzbcMjyDKRUFBrhfRcNjauljlqolfDX', 'WPIuzORuNbTGTNb', 'uDhqeVa', 'fQRglSARIviYABcjGeLK']) from system.numbers limit 10;
select [2, 0, 4, 5, 1, 15, 1, 9, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('BEwjMzphoTMoGikbrjTVyqDq'), ['E', 'sClgniMsZoGTEuLO', 'jmzphotmoGIKBRjtv', 'MZPhOtmo', '', 'Kb', '', 'otm', 'tVpxYRttoVpRLencV', 'SJAhAuMttGaeMsalRjeelAGG']) from system.numbers limit 10;
select [1, 0, 0, 0, 0, 0, 4, 0, 0, 19, 0, 7] = multiSearchAllPositionsCaseInsensitive(materialize('yNnYRQfcyemQdxUEPOiwRn'), ['', 'SJteoGNeIAMPWWBltkNKMrWDiVfR', 'kKnnKQhIPiekpnqTXJuyHfvWL', 'GPDUQEMWKzEEpvjLaIRYiuNfpzxsnSBX', 'oPrngRKwruyH', 'ukTSzFePSeVoeZeLQlAaOUe', 'yRqfcyemQDXUepo', 'CwmxidvpPHIbkJnVfSpbiZY', 'FUxmQdFVISApa', 'iwr', 'ciGHzDpMGNQbytsKpRP', 'Fcy']) from system.numbers limit 10;
select [0, 1, 0, 11, 2, 0, 1, 3, 0, 0, 0, 21] = multiSearchAllPositionsCaseInsensitive(materialize('EgGWQFaRsjTzAzejYhVrboju'), ['DVnaLFtCeuFJsFMLsfk', '', 'thaqudWdT', 'Tzazejy', 'GGW', 'RolbbeLLHOJpzmUgCN', '', 'gwqfarsjtzaZeJYHvR', 'KkaoIcijmfILoe', 'UofWvICTEbwVgISstVjIzkdrrGryxNB', 'UJEvDeESWShjvsJeioXMddXDkaWkOiCV', 'B']) from system.numbers limit 10;
select [0, 5, 2, 0, 0, 7, 0, 0, 0, 11, 0, 12, 22, 10, 0, 12] = multiSearchAllPositionsCaseInsensitive(materialize('ONgpDBjfRUCmkAOabDkgHXICkKuuL'), ['XiMhnzJKAulYUCAUkHa', 'dbj', 'nGpDbJFRU', 'xwbyFAiJjkohARSeXmaU', 'QgsJHnGqKZOsFCfxXEBexQHrNpewEBFgme', 'JFruCM', 'DLiobjNSVmQk', 'vx', 'HYQYzwiCArqkVOwnjoVNZxhbjFaMK', 'Cm', 'ckHlrEXBPMrVIlyD', 'M', 'xI', 'UcmkAOabdKg', 'jursqSsWYOLbXMLQAEhvnuHclcrNcKqB', 'mKaoaBdKghxiCkkUUL']) from system.numbers limit 10;
select [0, 1, 0, 1, 0, 0, 0, 0, 7, 21] = multiSearchAllPositionsCaseInsensitive(materialize('WhdlibCbKUmdiGbJRshgdOWe'), ['kDPiHmzbHUZB', '', 'CukBhVOzElTdbEBHyrspj', '', 'QOmMle', 'wiRqgNwjpdfgyQabxzksjg', 'RgilTJqakLrXnlWMn', 'bSPXSjkbypwqyazFLQ', 'CBkuMDiGbJRShGdOWe', 'dow']) from system.numbers limit 10;
select [0, 8, 0, 1, 1, 0, 1, 7, 0, 0, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('oOccAoDDoPzHUyRqdWhJxNmATEqtE'), ['LFuvoQkVx', 'DoPzh', 'YaBSTdWvmUzlgRloppaShkRmLC', 'oO', '', 'eeEpOSLSXbyaOxTscOPoaTcKcchPmSGThk', '', 'dDO', 'oFXmyIJtmcSnebywDlKruvPUgmPFzEnMvA', 'vCs', 'MsxHLTgQcaQYZdPWJshIMWbk', 'yqrjIzvrxd']) from system.numbers limit 10;
select [0, 16, 0, 0, 0, 0, 7, 1, 0, 0, 1, 2, 1, 4, 0, 3] = multiSearchAllPositionsCaseInsensitive(materialize('FtjOSBIjcnZecmFEoECoep'), ['FQQwzxsyauVUBufEBdLTKKSdxSxoMFpL', 'EOecoEP', 'HGWzNTDfHxLtKrIODGnDehl', 'ZxirLbookpoHaxvASAMfiZUhYlfuJJN', 'mKh', 'GZaxbwVOEEsApJgkLFBRXvmrymSp', 'Ij', '', 'X', 'AnCEVAe', 'fTj', 'tjOSbIjcNZECMfeoEC', '', 'OsBIjcN', 'LtdJpFximOmwYmawvlAIadIstt', 'JOsBiJCNzEc']) from system.numbers limit 10;
select [0, 2, 0, 0, 19, 0, 0, 12, 1, 0, 3, 1, 0, 0] = multiSearchAllPositionsCaseInsensitive(materialize('ugpnWWncvqSLsYUCVXRZk'), ['yOWnQmZuhppRVZamgmRIXXMDQdeUich', 'gPNww', 'jlyFSbvmjaYPsMe', 'fQUeGVxgQdmPbVH', 'rZk', 'ariCX', 'grAffMPlefMQvugtAzN', 'LsYuCVX', '', 'jZFoQdWEWJFfSmNDqxIyNjvxnZJ', 'P', 'UgPN', 'JmKMsbegxNvusaiGGAZKglq', 'qArXLxzdYvabPv']) from system.numbers limit 10;
select [0, 0, 0, 0, 0, 0, 8, 0, 0, 1, 1, 15, 0, 1, 7, 0] = multiSearchAllPositionsCaseInsensitive(materialize('nxwotjpplUAXvoQaHgQzr'), ['ABiEhaADbBLzPwhSfhu', 'TbIqtlkCnFdPgvXAYpUuLjqnnDjDD', 'oPszWpzxuhcyuWxiOyfMBi', 'fLkacEEeHXCYuGYQXbDHKTBntqCQOnD', 'GHGZkWVqyooxtKtFTh', 'CvHcLTbMOQBKNCizyEXIZSgFxJY', 'PlUAxVoQah', 'zrhYwNUzoYjUSswEFEQKvkI', 'c', 'NXWOt', '', 'qAhG', 'JNqCpsMJfOcDxWLVhSSqyNauaRxC', '', 'PpLuaxV', 'DLITYGE']) from system.numbers limit 10;
select [2, 0, 0, 1, 0, 0, 28, 1, 16, 1] = multiSearchAllPositionsCaseInsensitive(materialize('undxzJRxBhUkJpInxxJZvcUkINlya'), ['ndxzjRxbhuKjP', 'QdJVLzIyWazIfRcXU', 'oiXcYEsTIKdDZSyQ', 'U', 'dRLPRY', 'jTQRHyW', 'Y', '', 'nxxJZVcU', '']) from system.numbers limit 10;
select [1, 4, 1, 0, 4, 1, 0, 1, 16, 1, 0, 0, 0, 8, 12, 14, 0, 2] = multiSearchAllPositionsCaseInsensitive(materialize('lrDgweYHmpzOASVeiFcrDQUsv'), ['', 'gwEYhMP', 'LrDGwEyHmPzOaSVEifC', 'oMN', 'gwEYhMpZO', 'lrdGWEy', 'pOKrxN', 'lrDgwEyhmpZoaSv', 'eifcrdqU', 'LrDgw', 'dUvarZ', 'giYIvswNbNaBWprMd', 'pPPqKPhVaBhNdmZqrBmb', 'hmPzoASVEiF', 'O', 'SVEi', 'gIGLmHnctIkFsDFfeJWahtjDzjPXwY', 'rDGweyHmP']) from system.numbers limit 10;
select [0, 0, 11, 1, 1, 1, 0, 16, 0, 1, 5, 0, 0, 0, 2, 0, 2, 0] = multiSearchAllPositionsCaseInsensitive(materialize('XAtDvcDVPxZSQsnmVSXMvHcKVab'), ['bFLmyGwEdXiyNfnzjKxUlhweubGMeuHxaL', 'IhXOeTDqcamcAHzSh', 'ZSQsNMvsxmVHcK', '', '', '', 'dbrLiMzYMQotrvgwjh', 'MvsxMV', 'zMp', 'XaTDvCdvpXzsqSNMVSxm', 'v', 'LkUkcjfrhyFmgPXPmXNkuDjGYlSfzPi', 'ULpAlGowytswrAqYdaufOyWybVOhWMQrvxqMs', 'wGdptUwQtNaS', 'ATdVcdVPXzSqsnmVSXMvHcKVab', 'JnhhGhONmMlUvrKGjQcsWbQGgDCYSDOlor', 'atdvCdvpXzsqSnMVSxMVhCkvAb', 'ybNczkKjdlMoOavqBaouwI']) from system.numbers limit 10;
select [8, 0, 0, 0, 4, 0, 0, 5, 5, 2] = multiSearchAllPositionsCaseInsensitive(materialize('XPquCTjqgYymRuwolcgmcIqS'), ['qgyYMruW', 'tPWiStuETZYRkfjfqBeTfYlhmsjRjMVLJZ', 'PkTdqDkRpPpQAMksmkRNXydKBmrlOAzIKe', 'wDUMtn', 'UcTJQgYYMRuWoLCgMcI', 'PieFD', 'kCBaCC', 'Ct', 'C', 'pQuctjqgyymRuwOLCgmc']) from system.numbers limit 10;

select [1, 0, 7, 1, 0, 24, 17, 0, 0, 0, 2, 0, 1, 7, 4, 1, 12, 8] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('гГБаДнФбпнЩврЩшЩЩМщЕБшЩПЖПчдт'), ['', 'таОХхрзИДжЛСдЖКЧжБВЩжЛкКХУКждАКРеаЗТгч', 'Ф', '', 'ЙЩИФМфАГщХзКЩЧТЙжмуГшСЛ', 'ПЖпчдТ', 'ЩМщЕбшЩПжПч', 'ФгА', 'гУД', 'зУцкжРоППЖчиШйЗЕшаНаЧаЦх', 'гбаДНФбПНЩВРЩШЩщМЩеБшЩпжПЧд', 'РДЧЖАбрФЦ', 'гГ', 'ФбпНщвр', 'адНфБПнщвРщШщщМщЕбШщ', 'ггб', 'ВРЩ', 'бПНщврЩш']) from system.numbers limit 10;
select [0, 12, 8, 0, 12, 0, 0, 10, 0, 8, 4, 6] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('айРВбЧБжКВИхБкчФЖЖНВнпФйФБДфЗ'), ['ЛрЦфуУДВК', 'хБкчфЖжНвнпфйфБдФ', 'жКВИХБкчФЖжНвнПф', 'кЖчвУцВСфЗБТИфбСжТИдРкшгзХвщ', 'хбк', 'штДезйААУЛчнЖофМисНЗо', 'нлнШЧВЙхОПежкцевчлКрайдХНчНб', 'вИХбкчфжжНВН', 'ЩдзЦТуоЛДСеШГфЦ', 'ЖКВихбКЧфжЖ', 'вбЧбЖкВихБкЧфЖжНВ', 'Чб']) from system.numbers limit 10;
select [18, 15, 0, 0, 0, 0, 5, 0, 14, 1, 0, 0, 0, 0, 0, 15] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('пМИОкоЗжГйНТПЙацччЧАЩгЕВБбЕ'), ['ЧЧАЩгЕВБ', 'а', 'ФбРВщшййпХдфаЗЖлЛСЗПРШПпАОинЧКзЩхждН', 'ЛфРКДЙВСУСЙОчтнИкРЗбСГфкЩреИхЛлчХчШСч', 'ШйвБПАдФдФепЗТкНУрААйеЧПВйТоЧмБГДгс', 'ФтЙлЖЕсИАХИФЗаЕМшсшуцлцАМФМгбО', 'КО', 'лиШБнлпОХИнБаФЩдмцпжЗИЛнвсЩЙ', 'йацччЧАщгевбБЕ', 'ПмИоКозжГйНТП', 'ИГНннСчКАИСБщцП', 'ПнжмЙЛвШтЩейХЛутОРЩжифбЗчгМУЛруГпх', 'ХжЗПлГЖЛйсбпрЩОТИеБвулДСиГзлЛНГ', 'учклЦНЕгжмщлжАшщжМд', 'ЩеПОЙтЖзСифОУ', 'АЦЧ']) from system.numbers limit 10;
select [10, 0, 1, 1, 6, 1, 7, 6, 0, 0, 0, 2, 12, 0, 6, 0, 4, 8, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('квхБнцхйзЕпйИмтЙхфзвгдФ'), ['еПйИМт', 'хгкиМжСБИТНщенЩИщНСкй', '', 'Квхб', 'цхЙЗЕПйИмТйХФЗ', 'к', 'хйЗЕПЙИмтй', 'Цх', 'нКлШбМЖГйШкРзадрЛ', 'ДштШвБШТг', 'СЦКйЕамЦщПглдСзМлоНШарУтМднЕтв', 'ВхБнцхйЗЕПйимТ', 'йимтЙХФЗВГД', 'жчссунЙаРцМкЖУЦщнцОЕхнРж', 'цХЙЗЕП', 'ОгНФдМЛПТИдшцмХИеКйРЛД', 'бнЦхЙ', 'ЙЗе', 'згЩищШ', 'фХлФчлХ']) from system.numbers limit 10;
select [0, 0, 0, 12, 0, 0, 27, 1, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('хНпсРТХВдтоЦчдлеФПвнЛгЗКлПйнМВ'), ['ШиБфЗШПДЧхОЩшхфщЗЩ', 'иГйСЧЗтШЛуч', 'АЗХЦхедхОуРАСВЙС', 'цчдЛЕфП', 'СДбйГйВЕРмЙЩЛщнжен', 'НДлцСфТшАщижгфмуЖицжчзегЕСЕНп', 'й', '', 'йлчМкРИЙиМКЙжссЦТцРГзщнхТмОР', 'ПРцГувЧкйУХггОгЖНРРсшГДрлЧНжГМчрХЗфЧЕ']) from system.numbers limit 10;
select [0, 0, 2, 0, 10, 7, 1, 1, 0, 9, 0, 2, 0, 17, 0, 0, 0, 6, 5, 2] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЙзЗжпжДЕСУхчйдттСЙзоЗо'), ['щОЙУшееЧщкхГККреБкВ', 'жВ', 'ззЖпждЕсУХчЙДТТсЙ', 'ЙЦШЦЙЖзХШРвнкЕд', 'УхчйДтТсйЗОз', 'дЕСу', '', '', 'дсцеррищндЗдНкжаНЦ', 'сУхчЙдттсйзОзО', 'ЦЖРжмц', 'ЗЗ', 'СгЛГАГЕЖНгщОеЖЦДмБССцЩафзЗ', 'Сйзоз', 'ЦГХТЕвЕЗБМА', 'пмВоиеХГжВшдфАЖАшТйуСщШчИДРЙБнФц', 'Оа', 'ждЕ', 'ПжДесу', 'ЗзЖПждЕСУ']) from system.numbers limit 10;
select [0, 0, 0, 0, 5, 1, 0, 6, 0, 1, 17, 15, 1, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('уФШЙбШТоХВбзЦцЖОЕКТлщхнЖГ'), ['цЛ', 'ууМ', 'ТИгЙолМФсибтЕМнетквЦИЩИБккйн', 'оФОаМогсХЧЦооДТПхб', 'бШтОХВбЗцЦЖоЕКтЛ', 'уфШйбШтоХ', 'фдтщрФОЦсшигдПУхЛцнХрЦл', 'ШтО', 'НИкИТрбФБГИДКфшзЕмЙнДЖОсЙпЩцщкеЖхкР', 'уфШЙБш', 'екТлщ', 'ЖоекТл', 'уфШйБшТоХвбз', 'ТуОхдЗмгФеТаафЙм']) from system.numbers limit 10;
select [0, 1, 6, 1, 0, 1, 0, 0, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('чМЩБЛЛПРлщкВУбПефХВФлАЗШао'), ['гаТкЛВнрвдПМоеКПОйр', 'ч', 'ЛпрЛЩКвуБпе', 'ЧмЩб', 'ц', '', 'жгаччЖйГЧацмдсИИВЩЩжВЛо', 'йГеЙнБзгнкЦЛБКдОЕЧ', 'ПоЦРвпЕЗСАШж', 'ЙОНЦОбиееО']) from system.numbers limit 10;
select [2, 0, 17, 1, 0, 0, 0, 5, 0, 4, 0, 0, 0, 0, 0, 2] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЕаЩичщМЦЖиЗБЛЧжуНМЧК'), ['АЩиЧЩ', 'ИлУсшДБнжщаатуРТтраПОЙКЩйТГ', 'НМЧк', 'Еа', 'зАВФЛЩбФрМВШбПФГгВЕвЖббИТйе', 'РЗНРБЩ', 'ЦдЙНГпефзЛчпУ', 'ч', 'НШШчПЗР', 'ИчЩмЦжИЗБлЧЖУНМч', 'аннвГДлмОнТЖЗЙ', 'ШдчЩшЕБвхПУсШпг', 'гФИШНфЖПжймРчхАБШкЖ', 'ЖзгЖАБлШЗДпд', 'Д', 'ащиЧ']) from system.numbers limit 10;
select [4, 1, 0, 7, 0, 7, 1, 1, 0, 3, 7, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('иОцХКЙвувМИжШдУМУЕйНсБ'), ['ХкйвуВмИжШдУм', '', 'звМАОМЩщЙПшкиТчЩдгТЦмфзеИ', 'вуВМиж', 'КДщчшЙВЕ', 'в', '', 'ИоЦхКЙВувМижШ', 'ЕвТАРи', 'цхКЙвувмИЖШДумуе', 'вУвМи', 'зПШИХчУщШХУвврХйсуЙЗеВЧКНмКШ']) from system.numbers limit 10;
select [0, 5, 0, 0, 0, 0, 0, 12, 0, 11] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЦОфбчУФсвТймЦчдщгЩжИАБ'), ['йлрк', 'ЧуФсвтйМцчдЩгщ', 'МНлЕжорв', 'иНзТЖМсмх', 'шЕМЖжпИчсБжмтЧЙчщФХб', 'жШХДнФКАЩГсОЩвЕаам', 'НпКЦХулЛвФчШЕЗкхХо', 'мЦчДЩгЩжиАб', 'мпцгВАЕ', 'Й']) from system.numbers limit 10;
select [1, 0, 0, 0, 8, 0, 2, 0, 0, 7] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('чТХЙНщФфцИНБаеЖкОвлиУДР'), ['', 'рВХмжКцНцИЙраштМппсодЛнЧАКуЩ', 'ИХфХЖЧХВкзЩВЙхчфМрчдтКздиОфЙжУ', 'Гзлр', 'фЦи', 'абПф', 'тХЙНщффЦИн', 'нссГбВеЖх', 'амлЗщрсУ', 'фФ']) from system.numbers limit 10;
select [0, 9, 11, 0, 11, 1, 0, 0, 0, 1, 6, 1, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('зДЗпщАцвТгРдврщхЩфЖл'), ['йХЛ', 'Т', 'рд', 'АИЦщгниДфВОе', 'Р', 'здзпщ', 'вКТвВШмгч', 'ввирАйбЗЕЕНПс', 'тХиХоОтхПК', '', 'аЦВТгРДврщ', '', 'уЗЗЖвУЕйтчудноЕКМЖцВРаНТЙЗСОиЕ', 'оЕфПхЕДжАаНхЕцЖжжофЦхкШоБЙр']) from system.numbers limit 10;
select [1, 1, 0, 0, 1, 7, 0, 0, 0, 2] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('йЛПЛшмЦШНЖРрЧрМцкЖзЕНжЧДелФжАн'), ['', 'йЛПлшМЦшНЖррч', 'ПНКдфтДейуиШзЗХАРУХизВ', 'ПценмщЧОФУСЙЖв', '', 'ЦшнжрРчрМЦКЖЗе', 'МрПзЕАгжРбТЧ', 'ЕДФмаФНвТЦгКТЧЦжцЛбещЛ', 'УтПУвЛкТасдЦкеИмОещНИАоИжЖдЛРгБЩнвЖКЛЕП', 'Л']) from system.numbers limit 10;
select [1, 5, 1, 1, 0, 0, 1, 1, 0, 2, 19, 0, 2, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('сйДпмжнДРщКБгфцЖОчтГНБ'), ['', 'МЖнДРщ', 'Сй', '', 'пУщ', 'йгВИАЦнозаемТиХВвожКАПТдкПИаж', 'Сйд', 'СЙДпмжНдРщ', 'ФПщБцАпетаЙФГ', 'ЙдпМжНдрЩКбГфЦжОЧТГНб', 'т', 'гллрБВМнвУБгНаЙцМцТйЙФпзЧОЙЛвчЙ', 'йДПМжндРЩкБ', 'ЗмфОмГСНПщшЧкиССдГБУсчМ']) from system.numbers limit 10;
select [0, 18, 10, 5, 0, 2, 8, 1, 4, 11] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ХпИРддХрмВНйфчвгШиЧМКП'), ['хЗФДлДУБЙаЦтжРБЗсуйнЦпш', 'иЧмК', 'внЙ', 'д', 'зиМУЩГиГ', 'ПИр', 'РМвнЙфчвгШич', '', 'РдДхРМ', 'нЙфчВГШИ']) from system.numbers limit 10;
select [18, 0, 0, 1, 0, 0, 6, 0, 0, 9] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('нГгФкдуФШуИТбшпХфтаГт'), ['Таг', 'рРпшУйчГд', 'гК', '', 'лаВНбездпШШ', 'ЕБРйаНрОБожкКИсв', 'ДУфШУитБ', 'ГРиГШфШтйфЖлРФзфбащМЗ', 'мхЩжЛнК', 'ШуИтБШ']) from system.numbers limit 10;
select [13, 0, 0, 7, 0, 15, 0, 0, 15, 0, 0, 5, 6, 0, 18, 21, 11, 1] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('рлобшдПЦИхжФуХщжгПФукшзт'), ['УхщжГ', 'ТВщЦфФсчЩГ', 'ЕжФШойжуЛРМчУвк', 'пцИХжфуХЩж', 'бР', 'щЖГПфуКШЗТ', 'йжРГгЛуШКдлил', 'ТщЖГкбШНИщЩеЩлаАГхрАфЙНцЦгВкб', 'щжГПфУ', 'бкаДБЛХ', 'АЗ', 'шДПЦихжфух', 'дП', 'вфнЙобСцвЩмКОбЦсИббФКзЩ', 'пФУкшзТ', 'К', 'жфу', '']) from system.numbers limit 10;
select [12, 19, 8, 1, 0, 0, 0, 15, 0, 0, 12, 2, 0, 4, 0, 0, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЦкЛЗепкЕХЩГлКФрБдТрлвйАхдООШ'), ['лК', 'рЛв', 'Ехщ', '', 'еаПКБгЦЩАоЗВонйТЗгМхццСАаодМЕЩГ', 'ишОНиеБидфбФБЖриУЩЩ', 'дуж', 'РбДТ', 'пЗсГХКсгРущкЙРФкАНЩОржФвбЦнЩНЖЩ', 'щрОУАГФащзхффКвЕйизцсйВТШКбнБПеОГ', 'лкФрБдТРлвЙа', 'КЛзеп', 'УЛФЗРшкРщзеФуМвгПасШЧЛАЦр', 'зеПКеХщглкфР', 'ЦЖЗдХеМЕ', 'зЖжрт', 'уЩФрйрЖдЦз', 'МфцУГЩтвПАЦжтМТоеищЕфнЖй']) from system.numbers limit 10;
select [0, 0, 1, 0, 1, 0, 0, 7, 0, 5, 1, 6, 1, 1, 1, 5, 6, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('РННЕШвжМКФтшДЙлфЛИзЙ'), ['ГаМРош', 'Дтфс', '', 'еБбиаКщГхххШвхМЖКзЛАезФУчХо', 'РНн', 'сВбТМ', 'ЖЗЦПБчиСйе', 'жМкфтШДЙл', 'нЖХуеДзтЧтулиСХпТпеМлИа', 'ШВжМкФТШдЙлфл', '', 'вЖМКфТ', '', '', '', 'швЖМКфтШДЙЛфлИЗй', 'вЖмКФТ', 'еМ']) from system.numbers limit 10;
select [0, 0, 15, 1, 0, 0, 8, 1, 0, 0, 0, 4, 8, 10] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('РиучГийдХутДЕЙДпфиуд'), ['ЩмгцлЖрц', 'ЕСжСлЩЧИЖгЗЛлф', 'дП', '', 'щГЦаБтПШВзЦСрриСЙбД', 'тдРгОЛТШ', 'д', '', 'КЕбЗКСХЦТщЦДЖХпфаЧйоХАл', 'мТвзелНКрЖЧЦПпЕЙвдШтеШйБ', 'ЙОТКрБСШпШд', 'ЧГ', 'ДХУТДЕЙд', 'УТд']) from system.numbers limit 10;
select [0, 0, 0, 0, 15, 0, 0, 0, 11, 0, 0, 5, 1, 1, 0, 2, 3, 0, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('МшазшргхОПивОлбмДоебАшцН'), ['ЦИшштН', 'еМСЗкФЕКДйОНМ', 'ЛСчГрбеРЕбЩМПМЗЦИп', 'ХнИПЧжЗдзФщЗ', 'бмдоЕ', 'гМОдйсбТСЦЩбФВЗШзшщбчегаЕмЕБаХаРР', 'фщнР', 'щмТчФчсМАОгчБщшг', 'иВ', 'УщцГОшТзпУХКоКЖБеМШ', 'мйаАЛцАегСмПОаСТИСфбЧДБКоИВчбЦЙ', 'шРгхоп', '', '', 'еИпАЩпнЛцФжЩХИрЧаИИТЛвшиСНЩ', 'шаЗ', 'АЗ', 'ФгдтфвКЩБреногуир', 'ДБжШгщШБЩпЖИЛК', 'ЧдРЩрбфЛзЙклхдМСФУЙЛн']) from system.numbers limit 10;
select [5, 0, 0, 18, 13, 0, 2, 7, 0, 0, 1, 15, 1, 0, 0, 0, 3, 0, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('хщеКЗПчуНЙтрЧЩгфСбоКЕАДТййАрр'), ['зп', 'хчПЦшпДбзСфНВЧзНжЕМФОП', 'ЧЖхЕУк', 'БОКеАдтЙЙа', 'чЩГфС', 'шллддЩщеМжШйкЩн', 'щЕкзпЧуНЙТ', 'ЧунйтРЧщгФс', 'ввНздЙуоТЖРаВЙчМИчхРвфЛЖБН', 'ЗХМХПщПкктцАзщЙкдпжф', '', 'ГФСбОкеАДтйЙа', '', 'МБХВЕчпБМчуххРбнИМЛТшЩИщЙгаДцзЛАМвйаО', 'ЛкОзц', 'ЕцпАДЗСРрсЕвтВщДвцбЗузУннТИгХжхрцПДРДПм', 'екЗПЧунЙТРчщгФсбоК', 'шпИфЕчгШжцГВСйм', 'ЛхйЧбЧД', 'ВзЗоМцкЩНХГж']) from system.numbers limit 10;
select [0, 0, 6, 20, 0, 10, 0, 0, 0, 9, 10, 3, 23, 1, 0, 0, 2, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('лцапШиХчЛДшдксСНИбшгикзчЙанми'), ['ХууатТдтбодМГЧгщЧнклШтЗПНчкЦОаЙг', 'МЦЧчпИхКЛаФхщХдРУДщжУчфлжахц', 'иХЧлдшдкСсНИбШГикзЧЙ', 'гикЗчйА', 'ГсТзЛОфИББлекЩАсЛвмБ', 'Д', 'ЦХрТЖощНрУШфнужзжецсНХВфЩБбДУоМШШиГйж', 'йуВдЕзоггПВДЖб', 'ЙфБГйХМбжоакЖЛфБаГИаБФСнБЖсТшбмЗЙТГОДКИ', 'ЛДШдКССНИБшГикзч', 'ДШдКССниБ', 'аПШИХчЛДШДКсс', 'з', '', 'ФоохПЩОГЖоУШлКшзЙДоуп', 'хАДХЩхлвУИсшчрбРШУдФА', 'ЦА', 'гвптУФлчУуРхпрмЖКИрБеЩКчН']) from system.numbers limit 10;
select [0, 4, 5, 7, 15, 3, 3, 17, 7, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('зЗАЩлЕЕЕПИохЧчШвКЧйрсКХдд'), ['пКРбуШОНТЙБГНзИРвЖБсхрЛщчИрлЧУ', 'ЩЛЕЕЕПиоХЧ', 'ЛеЕеп', 'Еепио', 'швкЧйрС', 'ащЛеееПИох', 'АЩлеЕЕпиОхЧЧШвкЧЙРсК', 'КчйРскхД', 'ЕЕПИохччшВКчй', 'у']) from system.numbers limit 10;
select [1, 12, 0, 8, 1, 1, 0, 1, 5, 0, 1, 0, 0, 0, 0, 3, 1, 0, 4, 5] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ПмКСйСКЖККмШеоигЙчПфжТ'), ['', 'Шео', 'РчвлдЙЙлПщуКмтН', 'жкКмшЕоИГЙЧ', '', '', 'йРмМЖнПиЙ', '', 'йс', 'тфФРСцл', '', 'щлЩХиКсС', 'кпнТЖпФЩиЙЛ', 'абкКптбИВгмЧкцфЦртЛДЦФФВоУхЗБн', 'чНшоВСГДМйДлтвфмхХВВуеЩЦВтЖтв', 'кС', '', 'фидБлйеЙЧШРЗЗОулщеЕЩщЙсЙшА', 'СЙс', 'йсКжкКМшЕо']) from system.numbers limit 10;
select [0, 0, 1, 0, 2, 2, 1, 2, 7, 0, 1, 2, 1, 0, 6, 8] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('УгЖЕугАЩХйчидаррлжНпфФГшр'), ['утвШ', 'кЕвФч', 'угжеУг', 'тШлТвЕШЗчЖеЛНджЦазЩХцж', 'гЖеугаЩхй', 'ГжЕугаЩХйЧидАР', 'УгжЕУГаЩХЙЧИда', 'гЖеу', 'ащхЙчИ', 'мЧлщгкЛдмЙЩРЧДИу', '', 'ГжеугАщХйЧиДаРРЛЖНП', '', 'зЕМвИКбУГКЩФшоГЧГ', 'ГАЩХйчИДАррлЖНпФфг', 'ЩХЙчИдАррЛЖНпфФгш']) from system.numbers limit 10;
select [1, 0, 0, 7, 0, 6, 0, 11, 0, 0, 0, 2, 0, 0, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЗЕГЛЩПцГНтзЕЦШЧхНКГТХЙЙФШ'), ['', 'шзкиЗсаИщАБмаз', 'Ж', 'ц', 'гШуЕжЛСПодРнхе', 'пцГНтЗЕЦ', 'щРкЩАеНржЙПМАизшщКвЗщглТкКИф', 'ЗеЦшчхнКГтхЙЙ', 'пелгЩКкцвтфнжЖУуКосЙлкЛ', 'рф', 'хНШчНрАХМШщфЧкЩБНзХУкилЙмП', 'ЕгЛЩПЦгнтзецШЧ', 'ЩУчБчРнЖугабУоиХоИККтО', 'СГмЦШтФШЛмЙЩ', 'ауТПЛШВадоХМПиБу', 'ЩЩйр']) from system.numbers limit 10;
select [2, 2, 1, 0, 0, 0, 0, 0, 1, 0, 7, 9, 0, 15, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('гЙЧЙФХнЖБвомгАШГбОВГксИйцз'), ['ЙЧйфхНЖбвО', 'Й', 'гЙЧйфхнЖбв', 'хсЩмШЙЙММВЦмУБТчгзУЛР', 'зктшп', 'дЕоиЖлгШж', 'хКкаНЛБ', 'ЗКйСчсоЗшскГЩбИта', '', 'у', 'НжбВОмгашГ', 'БВо', 'ещфРШлчСчмаЖШПЧфоК', 'шгбо', 'ЙСтШШДЩшзМмдпЧдЙЖевТвоУСЕп', 'Л']) from system.numbers limit 10;
select [0, 9, 0, 0, 18, 13, 13, 11, 0, 0, 4, 1] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЙЛмоЦСдТаоФчШКЖЦСНРаРЦзоС'), ['ДфгЗАасВфаМмшхчлмР', 'аоФчШкЖцСнРАРЦзОС', 'зЩзнйтФРТЙжУлхФВт', 'чЦкШВчЕщДУМкхЛУЩФшА', 'н', 'Шк', 'шКЖцсНРаРцЗос', 'фчшкЖцснрАРЦз', 'лку', 'пЧШМЦквоемЕщ', 'о', 'йЛМоцСДТАофЧшкжЦСнРаРЦзос']) from system.numbers limit 10;
select [21, 0, 0, 17, 1, 11, 0, 2, 0, 7] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('кЧЖнЕбМЛпШЗХиЙжиМщлнСФрПЧЖВН'), ['сФ', 'гцХаШЛсаШЛкшфЧОКЛцзешХСиЩоаЕОш', 'Г', 'МщЛНСФРпч', '', 'зХ', 'ОАДепНпСГшгФАЦмлуНуШШЗфдЧРШфрБЛчРМ', 'чЖне', 'СфЕАбФн', 'М']) from system.numbers limit 10;
select [4, 0, 1, 1, 0, 2, 4, 16, 3, 6, 5, 0, 0, 6, 1, 0, 5, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('кдАпЩСШИСцРхтеСиФЖЧСсОоц'), ['пщСшиСцрХТЕсифЖчССоОц', 'рхнкикДТКДВШчиЖЦнВм', '', '', 'жПЛСнЦцн', 'дА', 'ПщсШИсцрХтЕс', 'иФжЧсСоОЦ', 'ап', 'с', 'щсШИ', 'МАзашДРПЩПзРТЛАсБцкСШнЕРЙцИЩлТЛеУ', 'ичцпДбАК', 'сшИСЦрхтЕсифжчСсООц', 'КдАПЩСшИСЦРХТЕсИфЖЧСсо', 'ЛнБсИПоМЩвЛпиЩЗЖСд', 'щс', 'шщДНБаСщЗАхкизжнЛАХЙ']) from system.numbers limit 10;
select [0, 13, 0, 2, 16, 1, 3, 0, 9, 0, 2, 0, 1, 4, 0, 0, 0, 1] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('иНхеЕкхЩщмгзМГхсгРБсхОКцУгуНБ'), ['ДиоУлФЖЛисУСЕтсЕалщн', 'МгХсгрБСХО', 'ЖХНцршПшгйО', 'нХЕЕкхЩ', 'сГРбсхОКцУг', '', 'х', 'Ж', 'щМгЗмгхСг', 'СрпхДГОУ', 'НхеЕкХщ', 'ПМтБцЦЙЖАЙКВБпФ', 'ИнхеЕ', 'еЕКхЩ', 'мМГлРзш', 'гтдоЙБСВещкЩАЩЦйТВИгоАЦлчКнНРНПДЖшСЧа', 'ЖшеН', '']) from system.numbers limit 10;
select [1, 5, 0, 0, 3, 0, 2, 0, 14, 14, 1, 0, 17, 13, 3, 25] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('айлзсЗБоГйтГжЙРККФхКшлНРОрЦкфо'), ['', 'с', 'Д', 'шиБраНИЦЧуИжп', 'Лз', 'ДРБСУфКСшцГДц', 'йЛЗСЗбОгЙтГЖйРК', 'ЕЙЦсвРЕШшщЕЗб', 'ЙркКфхкшЛнРОР', 'ЙРкКФхкШ', 'а', 'ГдоДКшСудНл', 'КФхКшлНРоР', 'ж', 'лзСзБогйТГЖйрККф', 'оР']) from system.numbers limit 10;
select [6, 0, 8, 10, 1, 0, 1, 13, 0, 0, 0, 2, 2, 0, 4, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('РучУлрХчЗУпИчДТЕфщИЙщрНлн'), ['РХЧ', 'оДсГСЛЙшйиЧРСКзчХВоХарцНШ', 'ЧЗУпИ', 'УПичдТе', 'Р', 'ВЙЩхжАутПСНЦфхКщеЩИуЧдчусцАесзМпмУв', '', 'ЧдТ', 'ООсШИ', 'ФШсВжХтБУШз', 'ЕЩуДдшкМУРЕБшщпДОСАцйауи', 'УЧ', 'УЧУЛрХчзуПИчдТеФщий', 'йнЦцДСхйШВЛнШКМСфмдЩВйлнеЖуВдС', 'улрхчзупиЧдтефщИ', 'СХТЧШшГТВвлЕИчНОВи']) from system.numbers limit 10;
select [0, 0, 0, 2, 1, 1, 0, 1, 19, 0, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('УецжлЦЦщМшРГгЩЩдБмхЖЗЧзШЙб'), ['НзИуАузуРЗРуКфоТМмлПкрсмЕЕЕнТ', 'ЕЩГХхЧш', 'ХоЙпООчфЖввИжЙшЖжЕФОтБхлВен', 'ЕЦЖЛЦцщ', '', '', 'ухогСИФвемдпаШЗуЛтлизОЧ', 'УецЖ', 'ХЖзЧЗ', 'П', 'мБкзХ', 'уБуОБхШ']) from system.numbers limit 10;
select [6, 1, 15, 5, 0, 0, 0, 3, 2, 4, 0, 12, 0, 2, 0, 3, 1, 6, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ГЖФеачМаКчПСпкВкхсПтг'), ['чмАкЧ', '', 'ВкХс', 'ачМА', 'КлтжУлОЛршБЕблФЩ', 'тцуМфж', 'л', 'фе', 'Жф', 'ЕАЧМак', 'лЖЕРТнФбЧЙТййвзШМСплИхбЙЛЖзДпм', 'СпкВК', 'ЩзчжИш', 'жФеАчМ', 'КбЦбйЕШмКтЩЕКдуЩтмпИЕВТЖл', 'ФЕаЧмАКчПСПквкхспТ', 'гжФеАЧмаКчпСп', 'ЧмАК', 'дцкДННМБцйЕгайхшжПГх', 'ТЩбвЦЖАНшрАШФДчОщй']) from system.numbers limit 10;
select [1, 6, 0, 1, 0, 0, 3, 1, 2, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('МФННЧйОнцЛИЧЕПШПЧйоГФО'), ['', 'йОн', 'шУлгИЛЛРЙАсфЗоИЙЗРхуПбОЙсшдхо', 'МФННчЙоНц', 'лзВжбЦзфкзтуОйзуЗ', 'ЖГДщшЦзсжщцЦЖеЧвРфНИНОСАОщг', 'ННчйОНЦлИчЕПШ', '', 'Ф', 'ЩрИдНСлЙуАНЗвЕчмчАКмФУипндиП']) from system.numbers limit 10;
select [5, 0, 8, 13, 0, 0, 0, 1, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('зВйймХЩМзЦГЕкЕКфоСтхПблуКМхц'), ['МХщмз', 'НАНрШоНДмурМлО', 'мзцгЕкек', 'кеКфоСтХПбЛУК', 'СУУксО', 'ЦоШжЧфйШЦаГЧйбЛШГЙггцРРчт', 'НбтвВбМ', '', 'тЩФкСтоСЧЦЦЙаСДЩСГЙГРИФЗОЗфбТДЙИб', 'ВГж']) from system.numbers limit 10;
select [0, 0, 0, 8, 19, 0, 3, 12, 1, 4] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ДпбЙЖНЗбПнЛбахБаХТуабШ'), ['цИаЩвгеИР', 'Ф', 'РЖиА', 'БпнЛб', 'У', 'Тфн', 'Б', 'БА', '', 'ЙЖНзБПнлбАхбаХ']) from system.numbers limit 10;
select [0, 0, 0, 0, 0, 1, 0, 17, 1, 0, 1, 1, 1, 11, 0, 1, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ТЦмЩОинХзоДДпПНЩигрРщОзКц'), ['ЕжЙВпПл', 'ВКфКТ', 'ШкДсЖхшфоПИадУбхФЩБчОАкпУеБхи', 'НТЕЙОШЦЖоЩбзВзшс', 'учГгуКФзлУдНУУуПУлкаЦЕ', '', 'фАПМКуЧйБЧзСоЗргШДб', 'ИГРрщОзк', '', 'йупОМшУйзВиВрЛЩЕеЩмп', '', '', '', 'дДППнщИгРР', 'ШФвИЧакеЦвШ', 'ТцМЩоинхЗОДдппнЩ', 'мрОгЩшЩеЧ', 'еЖРиркуаОТсАолЩДББВАМБфРфпШшРРРм']) from system.numbers limit 10;
select [3, 0, 0, 0, 0, 0, 1, 0, 0, 14, 0, 1, 0, 1, 1, 1, 0, 7] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('аОкиЛгКйхаОГОУзЦЛрбцш'), ['кИЛГкйхАогоУЗЦл', 'щЧДпХИхбпсГвфДФХкчХ', 'ШвАмБЗлДОИПткжхФТФН', 'щфсхФмЦсЛеувЙО', 'лВУЖц', 'еИщРшозЖАдцтКииДУлДОУФв', 'а', 'ХгЦРШ', 'ФзрЖкРЗЩЧИеЧцКФИфЧЧжаооИФк', 'уЗ', 'фЦФдцжжМчЗЖлиСЧзлщжжЦт', '', 'МдхжизИХфвбМААрйФНХдЕжп', 'аОкиЛг', 'АОКИЛгкйХАОГОУЗЦ', '', 'МбЖйрсумщиеОЩк', 'КйХАоГоУЗцлРБЦШ']) from system.numbers limit 10;
select [0, 0, 2, 1, 0, 0, 12, 0, 17, 0, 0, 0, 2, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('КУчЛХФчЛХшвбМЦинРвНрФМРкмиеЕп'), ['ТБЩБзхАмщПщЧПИФПашгЕТиКЦМБМпСЩСуЩМчтшеш', 'йлВЕЙшфшаШЗШЩВХЦчЛБс', 'УЧл', '', 'ЛДсЖщмНЦсКуфЗуГиука', 'РТТОТфГЕлЩЕгЛтДфлВЖШГзЦЖвнЗ', 'БМцИНРвнРф', 'ОЕИЕдИсАНаифТПмузЧчЖфШЕуеЩсслСШМоЖуЩЛМп', 'рвНРфМркМи', 'ЦзБМСиКчУжКУЩИИПУДвлбдБИОЙКТЛвтз', 'злСГе', 'ВдтцвОИРМЕжХО', 'учЛХфЧл', 'БшччШбУзЕТзфКпиШжнезвоеК']) from system.numbers limit 10;
select [0, 7, 0, 0, 0, 0, 7, 6, 0, 16, 12, 12, 15, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('оЖиогсфклШМСДрбхРбМбрЕщНЙЗйод'), ['иПмДКейууОклНХГЗсбаЙдШ', 'ФКлШмсДрБХРбМбрещНЙЗЙОд', 'арчжтСТнк', 'чбТНЛЕжооЗшзОУ', 'ощАЩучРСУгауДхГКлмОхЙцЕо', 'аЛбкиЦаКМбКхБМДнмФМкйРвРр', 'ФКлШмСДрбХРбм', 'СфклШ', 'еДйилкУлиИчХЙШтхцЗБУ', 'хрБ', 'СДрбХрбМБР', 'СдрбхРБ', 'бхрБМБРЕщНйз', 'КИб']) from system.numbers limit 10;
select [22, 1, 8, 0, 0, 1, 0, 3, 0, 6, 20, 0, 0, 0, 4, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('ЕЖДФбКужЙЦЦмсЖГГжБзеЙнПйЙри'), ['НПййР', '', 'Жй', 'Щ', 'ФхУО', 'ЕЖДфБКУЖйЦЦмСжГГ', 'НФЙзщЩГЧпфсфЦШОМЕЗгцрс', 'д', 'ЦтщДДЖтбвкгКонСк', 'кУЖЙЦЦм', 'ЕйНПййРИ', 'РчеЙйичФбдЦОтпчлТЖИлДучЙПгЗр', 'внчзшЗзОнФфхДгфзХТеНПШРшфБТЖДйф', 'кНснгмулМуГНурщЕББСузВмбнЧаХ', 'фбКУЖйЦцМсЖГгЖб', 'ЩСЕ']) from system.numbers limit 10;
select [0, 0, 0, 1, 10, 4, 0, 0, 5, 0, 1, 0, 7, 0, 3, 7, 0, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('чБхлжгКЖХлЙнкКЦфжЕгЖАндЧ'), ['ПдмРрЖАтВнСдСБШпПЗГгшИ', 'цшцг', 'тчАЙЧОеЕАвГпЗцЖЧгдХуЛСЛНрвАЖщ', '', 'Лй', 'Л', 'ОйррцУжчуЦБАжтшл', 'вХУКк', 'жгКжхЛЙН', 'уцбЕЕОЧГКУПуШХВЕчГБнт', '', 'ПсАжБИКштЕаН', 'КжхлЙН', 'ЩгШухЦПАТКежхгХксгокбщФЙПсдТНШФЦ', 'Х', 'кЖХЛйНккЦФжЕГЖ', 'ЙзРДСПднаСтбЧЖхощ', 'пАПОУЧмИпслБЗПфУ']) from system.numbers limit 10;
select [0, 0, 0, 5, 2, 16, 4, 4, 11, 0, 0, 3, 3, 0, 0, 6] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('кпМаоуГГфвощолЦЩщЧПРОКепеА'), ['ЗзуФжНшщПТнЧЦКВОиАУсЧХОШбк', 'тмПкАпеайзуХсурШй', 'АЕЦавбШиСДвВДумВкиИУБШЕ', 'о', 'ПМаОУггФВощоЛЦЩЩЧПрокЕПеа', 'щЩ', 'аоУг', 'аОуГгФВ', 'оЩоЛЦЩщчПРОК', 'виХЛшчБсщ', 'УчАМаЦкйДЦфКСмГУЧт', 'мАоУ', 'МАО', 'щФФА', 'Н', 'У']) from system.numbers limit 10;
select [0, 3, 10, 8, 3, 0, 4, 0, 9, 4, 1, 9] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('щЙЧРпшИцхпргЦНуДййусЧЧнЖ'), ['ДлУцтееЖБКХгМзСВжА', 'чРпШИЦ', 'пргЦнУДЙЙУ', 'Ц', 'ЧРПш', 'нЩрЕвмрМеРйхтшЩче', 'РпШИЦхПРГцнУд', 'ПНоЙтПкоаОКгПОМЦпДЛФЩДНКПбСгЗНЗ', 'ХПРГцНудЙЙ', 'рПши', '', 'ХПРГ']) from system.numbers limit 10;
select [11, 4, 1, 0, 1, 0, 0, 0, 0, 12, 0, 9, 5, 0, 16, 0, 12, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('пкзщщЛНОНбфЦноИЧфхбФ'), ['ф', 'щщл', 'ПКзЩщЛНОн', 'ЩшФйЧБНДОИзМхеЖНЦцеЛлУЧ', '', 'сЗоЙТклйДШкДИЗгЖ', 'орЛФХПвБбУхНс', 'доЗмЩВу', 'ШиЕ', 'ЦНО', 'ндЩдРУЖШМпнзНссЖШДЦФвпТмуМЙйцН', 'НбФЦнОИч', 'ЩлНонБФ', 'ЛдРжКММЙм', 'чфх', 'ЦматДйиСфЦфааЦо', 'ЦНОИчФх', 'иржЦщн']) from system.numbers limit 10;
select [0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0, 3, 0, 5] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('чЖажцВбшЛттзДааАугШщАйПгщП'), ['ШгУТсчГОВЦЦеЛАСфдЗоЗЦВЛйлТДзчвЛва', 'УшЕшищЖткрвРСйиФЗйТФТЛЗаЗ', 'ВдикЙббщузоФХщХХГтЗоДпхбЕкМщц', 'срйеХ', 'рАшуПсЙоДнхчВкПЖ', '', 'гНЗбКРНСБВрАВФлнДШг', 'фХЧгмКнлПШлЩР', 'мкйЗбИФрЗахжгАдвЕ', 'чжаЖцВБШлТ', 'лХЕСрлПрОс', '', 'ЗЧПтчЙОцвОФУФО', 'ажцвБшЛТт', 'уНчЖШчМЕА', 'ц']) from system.numbers limit 10;
select [7, 1, 0, 7, 1, 19, 8, 6, 3, 0, 2, 13, 6, 0] = multiSearchAllPositionsCaseInsensitiveUTF8(materialize('НТКПпмБжДцбАКПНСЖоиТФД'), ['б', '', 'аУщЛМХЖбвИтНчГБМГдДнч', 'Б', 'НТкппм', 'и', 'Жд', 'МБждЦбАкП', 'кппмБждцБа', 'ПЕрнЦпМЦВгЧЧгГ', 'ткПпМБЖДцбаКпнСжО', 'кПнСЖоИ', 'МБжДцБакпН', 'гхОХжГуОвШШАкфКМщсшФДШеИжоАйг']) from system.numbers limit 10;

select 0 = multiSearchAny(materialize('mpnsguhwsitzvuleiwebwjfitmsg'), ['wbirxqoabpblrnvvmjizj', 'cfcxhuvrexyzyjsh', 'oldhtubemyuqlqbwvwwkwin', 'bumoozxdkjglzu', 'intxlfohlxmajjomw', 'dxkeghohv', 'arsvmwwkjeopnlwnan', 'ouugllgowpqtaxslcopkytbfhifaxbgt', 'hkedmjlbcrzvryaopjqdjjc', 'tbqkljywstuahzh', 'o', 'wowoclosyfcuwotmvjygzuzhrery', 'vpefjiffkhlggntcu', 'ytdixvasrorhripzfhjdmlhqksmctyycwp']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('qjjzqexjpgkglgxpzrbqbnskq'), ['vaiatcjacmlffdzsejpdareqzy', 'xspcfzdufkmecud', 'bcvtbuqtctq', 'nkcopwbfytgemkqcfnnno', 'dylxnzuyhq', 'tno', 'scukuhufly', 'cdyquzuqlptv', 'ohluyfeksyxepezdhqmtfmgkvzsyph', 'ualzwtahvqvtijwp', 'jg', 'gwbawqlngzcknzgtmlj', 'qimvjcgbkkp', 'eaedbcgyrdvv', 'qcwrncjoewwedyyewcdkh', 'uqcvhngoqngmitjfxpznqomertqnqcveoqk', 'ydrgjiankgygpm', 'axepgap']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('fdkmtqmxnegwvnjhghjq'), ['vynkybvdmhgeezybbdqfrukibisj', 'knazzamgjjpavwhvdkwigykh', 'peumnifrmdhhmrqqnemw', 'lmsnyvqoisinlaqobxojlwfbi', 'oqwfzs', 'dymudxxeodwjpgbibnkvr', 'vomtfsnizkplgzktqyoiw', 'yoyfuhlpgrzds', 'cefao', 'gi', 'srpgxfjwl', 'etsjusdeiwbfe', 'ikvtzdopxo', 'ljfkavrau', 'soqdhxtenfrkmeic', 'ktprjwfcelzbup', 'pcvuoddqwsaurcqdtjfnczekwni', 'agkqkqxkfbkfgyqliahsljim']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('khljxzxlpcrxpkrfybbfk'), ['', 'lpc', 'rxpkrfybb', 'crxp', '', 'pkr', 'jxzxlpcrxpkrf', '', 'xzxlpcr', 'xpk', 'fyb', 'xzxlpcrxpkrfybbfk', 'k', 'lpcrxp', 'ljxzxlpcr', 'r', 'pkr', 'fk']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('rbrizgjbigvzfnpgmpkqxoqxvdj'), ['ee', 'cohqnb', 'msol', 'yhlujcvhklnhuomy', 'ietn', 'vgmnlkcsybtokrepzrm', 'wspiryefojxysgrzsxyrluykxfnnbzdstcel', 'mxisnsivndbefqxwznimwgazuulupbaihavg', 'vpzdjvqqeizascxmzdhuq', 'pgvncohlxcqjhfkm', 'mbaypcnfapltsegquurahlsruqvipfhrhq', 'ioxjbcyyqujfveujfhnfdfokfcrlsincjbdt', 'cnvlujyowompdrqjwjx', 'wobwed', 'kdfhaoxiuifotmptcmdbk', 'leoamsnorcvtlmokdomkzuo', 'jjw', 'ogugysetxuqmvggneosbsfbonszepsatq']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('uymwxzyjbfegbhgswiqhinf'), ['lizxzbzlwljkr', 'ukxygktlpzuyijcqeqktxenlaqi', 'onperabgbdiafsxwbvpjtyt', 'xfqgoqvhqph', 'aflmcwabtwgmajmmqelxwkaolyyhmdlc', 'yfz', 'meffuiaicvwed', 'hhzvgmifzamgftkifaeowayjrnnzw', 'nwewybtajv', 'ectiye', 'epjeiljegmqqjncubj', 'zsjgftqjrn', 'pssng', 'raqoarfhdoeujulvqmdo']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('omgghgnzjmecpzqmtcvw'), ['fjhlzbszodmzavzg', 'gfofrnwrxprkfiokv', 'jmjiiqpgznlmyrxwewzqzbe', 'pkyrsqkltlmxr', 'crqgkgqkkyujcyoc', 'endagbcxwqhueczuasykmajfsvtcmh', 'xytmxtrnkdysuwltqomehddp', 'etmdxyyfotfyifwvbykghijvwv', 'mwqtgrncyhkfhjdg', 'iuvymofrqpp', 'pgllsdanlhzqhkstwsmzzftp', 'disjylcceufxtjdvhy']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('mznihnmshftvnmmhnrulizzpslq'), ['nrul', 'mshftvnmmhnr', 'z', 'mhnrulizzps', 'hftvnmmhnrul', 'ihnmshftvnmmhnrulizzp', 'izz', '', 'uli', 'nihnmshftvnmmhnru', 'hnrulizzp', 'nrulizz']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('ruqmqrsxrbftvruvahonradau'), ['uqmqrsxrbft', 'ftv', 'tvruvahonrad', 'mqrsxrbftvruvahon', 'rbftvruvah', 'qrsxrbftvru', 'o', 'ahonradau', 'a', 'ft', '', 'u', 'rsxrbftvruvahonradau', 'ruvahon', 'bftvruvahonradau', 'qrsxrbftvru', 't', 'vahonrada', 'vruvahonradau', 'onra']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('gpsevxtcoeexrltyzduyidmtzxf'), ['exrltyzduyid', 'vxtcoeexrltyz', 'xr', 'ltyzduyidmt', 'yzduy', 'exr', 'coeexrltyzduy', 'coeexrltyzduy', 'rlty', 'rltyzduyidm', 'exrltyz', 'xtcoeexrlty', 'vxtcoeexrltyzduyidm', '', 'coeexrl', 'sevxtcoeexrltyzdu', 'dmt', '']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('dyhycfhzyewaikgursyxfkuv'), ['sktnofpugrmyxmbizzrivmhn', 'fhlgadpoqcvktbfzncxbllvwutdawmw', 'eewzjpcgzrqmltbgmhafwlwqb', 'tpogbkyj', 'rtllntxjgkzs', 'mirbvsqexscnzglogigbujgdwjvcv', 'iktwpgjsakemewmahgqza', 'xgfvzkvqgiuoihjjnxwwpznxhz', 'nxaumpaknreklbwynvxdsmatjekdlxvklh', 'zadzwqhgfxqllihuudozxeixyokhny', 'tdqpgfpzexlkslodps', 'slztannufxaabqfcjyfquafgfhfb', 'xvjldhfuwurvkb', 'aecv', 'uycfsughpikqsbcmwvqygdyexkcykhbnau', 'jr']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('vbcsettndwuntnruiyclvvwoo'), ['dwuntnru', '', 'ttndwuntnruiyclvv', 'ntnr', 'nruiyclvvw', 'wo', '', 'bcsettndwuntnruiycl', 'yc', 'untnruiyclvvw', 'csettndwuntnr', 'ntnruiyclvvwo']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('pqqnugshlczcuxhpjxjbcnro'), ['dpeedqy', 'rtsc', 'jdgla', 'qkgudqjiyzvlvsj', 'xmfxawhijgxxtydbd', 'ebgzazqthb', 'wyrjhvhwzhmpybnylirrn', 'iviqbyuclayqketooztwegtkgwnsezfl', 'bhvidy', 'hijctxxweboq', 't', 'osnzfbziidteiaifgaanm']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('loqchlxspwuvvccucskuytr'), ['', 'k', 'qchlxspwu', 'u', 'hlxspwuvv', 'wuvvccucsku', 'vcc', 'uyt', 'uvv', 'spwu', 'ytr', 'wuvvccucs', 'xspwuv', 'lxspwuvvccuc', 'spwuvvccu', 'oqchlxspwuvvccucskuy']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('pjjyzupzwllshlnatiujmwvaofr'), ['lnatiujmwvao', '', 'zupzwllsh', 'nati', 'wllshl', 'hlnatiujmwv', 'mwvao', 'shlnat', 'ati', 'wllshlnatiujmwvao', 'wllshlnatiujmwvaofr', 'nat']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('iketunkleyaqaxdlocci'), ['nkleyaqaxd', 'etunkleyaq', 'yaqaxdlocci', 'tunkleyaq', 'eyaqaxdlocc', 'leyaq', 'nkleyaqaxdl', 'tunkleya', 'kleyaqa', 'etunkleya', 'leyaqa', 'dlo', 'yaqa', 'leyaqaxd', 'etunkleyaq', '']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('drqianqtangmgbdwruvblkqd'), ['wusajejyucamkyl', 'wsgibljugzrpkniliy', 'lhwqqiuafwffyersqjgjvvvfurx', 'jfokpzzxfdonelorqu', 'ccwkpcgac', 'jmyulqpndkmzbfztobwtm', 'rwrgfkccgxht', 'ggldjecrgbngkonphtcxrkcviujihidjx', 'spwweavbiokizv', 'lv', 'krb', 'vstnhvkbwlqbconaxgbfobqky', 'pvxwdc', 'thrl', 'ahsblffdveamceonqwrbeyxzccmux', 'yozji', 'oejtaxwmeovtqtz', 'zsnzznvqpxdvdxhznxrjn', 'hse', 'kcmkrccxmljzizracxwmpoaggywhdfpxkq']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('yasnpckniistxcejowfijjsvkdajz'), ['slkpxhtsmrtvtm', 'crsbq', 'rdeshtxbfrlfwpsqojassxmvlfbzefldavmgme', 'ipetilcbpsfroefkjirquciwtxhrimbmwnlyv', 'knjpwkmdwbvdbapuyqbtsw', 'horueidziztxovqhsicnklmharuxhtgrsr', 'ofohrgpz', 'oneqnwyevbaqsonrcpmxcynflojmsnix', 'shg', 'nglqzczevgevwawdfperpeytuodjlf']) from system.numbers limit 10;
select 0 = multiSearchAny(materialize('ueptpscfgxhplwsueckkxs'), ['ohhygchclbpcdwmftperprn', 'dvpjdqmqckekndvcerqrpkxen', 'lohhvarnmyi', 'zppd', 'qmqxgfewitsunbuhffozcpjtc', 'hsjbioisycsrawktqssjovkmltxodjgv', 'dbzuunwbkrtosyvctdujqtvaawfnvuq', 'gupbvpqthqxae', 'abjdmijaaiasnccgxttmqdsz', 'uccyumqoyqe', 'kxxliepyzlc', 'wbqcqtbyyjbqcgdbpkmzugksmcxhvr', 'piedxm', 'uncpphzoif', 'exkdankwck', 'qeitzozdrqopsergzr', 'hesgrhaftgesnzflrrtjdobxhbepjoas', 'wfpexx']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('ldrzgttlqaphekkkdukgngl'), ['gttlqaphekkkdukgn', 'ekkkd', 'gttlqaphe', 'qaphek', 'h', 'kdu', 'he', 'phek', '', 'drzgttlqaphekkkd']) from system.numbers limit 10;
select 1 = multiSearchAny(materialize('ololo'), ['ololo', 'ololo', 'ololo']);

select 1 = multiSearchAnyUTF8(materialize('иечбпрхгебилцмпфвжцс'), ['лцмпфвж', 'ечбпрхгебилц', 'фвж', 'мпфвж', 'вжцс', 'пфвжцс', 'ц', 'чбпрхгебил', 'илцмп', 'фвж', 'ечбпрхгеби', '', 'б', 'хгеб', '', '', 'ил', 'ебилцмпфвжцс']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('змейдмоодкшуищвеишчддуцпх'), ['здсщесгдкзмчбжчщчиоо', 'чфззцмудщхтфрмсзрвшйщ', 'рлунбнзрфубуббдочтвлзмпгскузохк', 'ктзлебцам', 'вчспмж', 'нгкк', 'гпзйа', 'щпйкччнабакцтлапсбваихншхфридб', 'афсузжнайхфи', 'йрздеучфдбсвпжохрз', 'ошбечпзлг', 'полшхидфр']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('лшнуухевгплвйужчошгнкнгбпщф'), ['гбпщф', 'б', 'ф', 'чошгнкнг', 'йужчо', 'гплвйужчошгнкн', 'бпщф', 'плвйужч', 'шгнкнг', 'хевгплвй', 'плвйужчошгн', 'вй', 'лвйужчошгнкнгбпщф', 'лвйужчошгнкн']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('кцпгуоойвщталпобщафибирад'), ['ойвщталпобща', 'щта', 'пгуоойвщтал', 'ф', 'общ', 'цпгуоойвщталпобща', 'побщ', 'ф', 'цпгуоойвщталпобщафиб', 'побщаф', 'лпобщафи', 'цпгуоойвщталпобщафи', 'пгуоойвщталпобщаф', 'талпоб', 'уоойвщталпо', 'гуоойвщтал', 'уоойвщталп', 'щ', '', 'цпгуоойвщталпобщафибирад']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('фвгйсеккзбщвфтмблщходео'), ['еккзбщвфтмблщходе', 'йсеккзбщвфтм', 'вфтмблщходео', 'вгйсеккзбщ', '', 'йсеккзбщвфт', 'бщвфтмблщход', 'ккзбщвфтмблщход', 'ккзбщвфтм', 'еккзбщвфтмблщходе', 'еккзбщвфтмблщх', 'вгйсеккзбщвф', 'оде', 'оде', '', 'бщвфтмблщх', 'б', 'йсеккзбщвфтмблщходео', 'вфтмблщ', 'кзбщ']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('хбаипфшнкнлтбшрскшщдувчтг'), ['хгшгднфуркшщвфгдглххс', 'цогчщки', 'тдмщшйзйхиквмб', 'етелфмшвмтзгеурнтбгчнщпмйпйжжциш', 'чсбк', 'ибащлшздеуревжйфуепфхкузбзао', 'дкмбщдсбжййсвгкхбхпшноншлщ', 'щхбеехнцегрфжжу', 'збфлпгсмащр', 'скчдигцнсзфрещйлвзнбнл', 'освзелагррдоортлрз', 'утхрч', 'йкбрвруенчччпшрнгмхобщимантешищщбж', 'жгивтеншхкцаргдасгирфанебкзаспбдшж', 'ййекжшщцщ', 'ефдсфбунйчдбуй', 'бвжцирзшмзщ', 'випжцщйзхнгахчсцвфгщзкдтвчйцемшлй', 'лдрфгвднеиопннтчсйффвлхемввег', 'бмтцжжеоебщупфчазпгхггцегнрутр']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('фбуоойпцщишщлбхчрсллзвг'), ['уччхщ', 'вщчсарфмйшгшпйфгмжугмщжкцщгйжзфл', 'кклл', 'лпнжирпсиуо', 'нчипзфщхнтштхйхщрпзитко', 'вйпсдергвцзсцсгмхпбз', 'чфщдфоилгцевпц', 'чааиае', 'чгингршжтчпу', 'щетбнгутшйсгмвмучдхстнбрптничихб']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('лйвзжфснтлгбгцерлзсжфещ'), ['зсжф', '', 'бгц', 'зжфснтлгбгц', 'л', 'цер', 'жфснтлгбгц', 'тлгбг', 'це', 'гбгцерл', 'нтлгбгцерлзсж', 'жфещ', 'взжфснтлг', 'фснтлгбгцерлзсжфещ', 'нтлгбгцерлзсж', 'зжфснтлгбг', 'взжфснтлгбгцерлз', 'взжфснтлгбгце']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('нфдцжбхуучеинивсжуеблмйрзцршз'), ['чеинивсжуеблм', 'жуебл', 'блмйрзцрш', 'цр', 'м', 'фдцжбхуучеинивсжуеблмйрзцр', 'нивсж', 'ивсжуеблмй', 'й', 'всжуеблмйрзцршз']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('всщромуцйсхрпчщрхгбцмхшуиоб'), ['муцйсхрп', '', 'уцйсхрп', 'сщромуцйсхрпчщ', 'схрпчщр', 'сщромуцйсхрп', '', 'уцйсхрпчщрхгбцмх', '', 'цмхшуиоб', 'гбц', 'пчщр', 'цйсхрпчщр', 'омуцйсхрпч', 'схрпчщрхгбцм', 'йсхрпчщрхгбцм', '', 'пчщрхгбцм', 'уцйсхрпчщрхгбцмх', 'омуцйсхрпчщ']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('уузшсржоцчтсачтедебозцвчвс'), ['бомбсзхйхкх', 'отвгстзихфойукарацуздшгбщеховпзкй', 'мфнев', 'вйийшшггилцохнзбхрлхи', 'втинбтпсщрбевзуокб', 'оиойвулхкзлифкзиххт', 'зацччзвибшицщрзиптвицзхщхкбйгшфи', 'кнузршшднмвтощрцвтрулхцх', 'рчбкагчкпзжвтбажиабиркдсройцл', 'щргчкзожийтпдзфч', 'щбошгщзсжтнжцтлкщитеееигзцлцсмч', 'сцкк']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('щчбслгзвйдйжрнщчвфшй'), ['пдашзбалйнзвузкдвймц', 'щхтшйоч', 'фднвфигозржаз', 'рйфопхкшщвщдвл', 'цдкйхтусожпешпджпатфуиткп', 'щпбчсслгщййлвскшц', 'жпснс', 'уиицуувешвмчмиеднлекшснчлйц', 'пххаедштхмчщчбч', 'ичтмжз', 'лсбкчу', 'бгфдвпзрл', 'йицц', 'цфйвфлнвопкмщк', 'бгщцвбелхефв', 'мймсвзаелхнжйчохомлизенфш', 'трйднхндшсщмпвщомашчнгхд', 'жфцнифлгдзйе', 'зспкшщщенбцжгл', 'рщтб']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('шщпееасбтхогвгвцниуевисгшгбч'), ['гпа', 'стимсркзебхрвфпиемзчзу', 'нзгофухвекудблкадбшшусбеулрлмгфнйгиух', 'кфиашфобакщворувгвкчавфзшх', 'гфпгщгедкмтгрдодфпуйддхзчждихгрчтб', 'тцтжр', 'рцйна', 'йцбпбдрреаолг', 'житсфосшлтгсщдцидгсгфтвлз', 'жвтнжедцфцтхжчщч']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('вхкшгфпфмнщаохтмизпврйопцуйзмк'), ['дтчбкхащаткифружжейабфйкйтрскбощиеч', 'фтоуабхмдааиснрбраттклмйонлфна', 'цадзиднщймшкщолттпгщбх', 'кштбчжтждпкцнтщвмухнлби', 'микудпдпумцдцгфахгб', 'ирик', 'емлжухвмк', 'чгуросфйдцшигцхжрухжпшдкфгдклмдцнмодкп', 'ттбнллквдувтфжвчттжщажзчлнбждчщцонцлуж', 'елцофйамкхзегхклйгглаувфтуувее', 'двкзчсифвтекб', 'шсус']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('йхцглкцвзтшщочпзмнчтуеао'), ['йечдай', 'дащжщзлосмй', 'афуккгугаазшрчпцнхщцтмлфф', 'чфтфскрфйщк', 'жлччкцшнфижтехппафхвщфс', 'бзжчв', 'щкщймнкщлпедидсу', 'оцбажцзшзйпптгщтфекртдпдзшодвойвох', 'йжддбссерхичгнчлкидвгбдзуфембрц', 'ктщвшкрщмдшчогхфхусдотсщтцхтищ', 'пшстккамнбнардпзчлшечхундргтоегцзр', 'нсрнфузгжррчнжначучиелебрб', 'шгжмквршжтккднгаткзтпвкгзхшйр', 'змквцефтулхфохбнхбакдичудфмйчп']) from system.numbers limit 10;
select 1 = multiSearchAnyUTF8(materialize('шждйрчйавщбйфвмнжоржмвдфжх'), ['ор', '', 'йрчйавщбйфвмнжо', 'вщбйфвмнжорж', 'ждйрчйавщбйфвмнжорж', 'йавщбйф', 'дф', 'вщбйф', 'бйфвмнжорж', 'мнж']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('кдшнсйршгвлицбенйбцфрсаччетфм'), ['асмун', 'йогкдчодиф', 'лштйбжнзфкикмпбитжшгкбоослщгзнщо', 'улштжцисцажзчштгжтфффабйлофедуфме', 'дрпгкчджихшзммймиамзфнуиорлищзгйвху', 'йиоршнйоввквбдвдзасма', 'члмвасмфрхжсхрбцро', 'лшкизщушборшчшастйсцкжцбонсшейрщ', 'масдфкршлупасвйфщфважсуфсейшзлащхрж', 'дгхшщферодщцнйна', 'цзфзждбавкжрткст', 'рфбожзееаце', 'кошомвгпрщсдквазчавожпечдиуйлщадфкгфи', 'бшпхнхсгшикеавааизцсйажсдийаачбхч']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('хтиелйтарквурйлжпеегфш'), ['зпмйвзуднцпвжкбмйрпушдуавднвцх', 'фбссчгчвжакуагдвижйтщтшоабпхабжш', 'щхшибаскрщбшрндххщт', 'сммрсцзмптисвим', 'цсргщфж', 'восжбшйштезвлкммвдхд', 'вбсапкефецщжквплуо', 'даеуфчвеби', 'бтптлжпин', 'шчддтнсйкщйщ', 'фжхщецпзчбйкц', 'цсвфпздхрщхцбуцвтег']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('апрчвзфжмбутццрйщкар'), ['индхжз', 'жилцовщччгстби', 'ажс', 'фктйамйтаг', 'шммнзачггоннксцушпчн', 'чдлйтзтоцдгзццисц', 'пнбтувщцдсчнщмсакрлгфмгрй', 'овмсйнщзушвщгуитщрхвйодф', 'бзлштезвлаижхбмигйзалчолшеунлц', 'фкжпеввгшгащз', 'тменбщжмсхщсогттршгек', 'чап', 'х', 'шкомегурлнйпшбщглав']) from system.numbers limit 10;
select 0 = multiSearchAnyUTF8(materialize('двхопооллаеийтпцчфжштнргкк'), ['йймчнздешхбццбжибопгктрнркевпиз', 'фйрохсамщцнмф', 'ййхфдпецжзгнуорвбплоахрфиле', 'пкллкацнвдббогг', 'йщдезамтжйзихщжмцлх', 'гдзувмщиеулиддердшпитвд', 'фхтунйшзхтщжтзхгцорошднпбс', 'фнситбеелцдкйщойлатиуухгффдвищсше', 'нзщщщндцрнищпхйвтбвмцтнуадцбву', 'вбщкапшнв', 'зйлмуимчскщнивтшлчмуузщепшйр', 'шжбвйдр', 'гддждбкначдттфшжшхпфиклртпгм', 'еншащцфафчнгбнщххнзочбтпушщорегшцзб', 'уунеущкззоетбучкц', 'щасифзоажребийещ', 'пщбххсдгйтт', 'хшсчуотрт', 'жкднйрозбцшужчшбкккагрщчхат', 'шачефцгч']) from system.numbers limit 10;

select 0 = multiSearchAnyCaseInsensitive(materialize('QWyWngrQGrDmZxgRnlOMYHBtuMW'), ['ZnvckNbkeVHnIBwAwpPZIr', 'NCzFhWQmOqIGQzMORw', 'tDYaxfQXWpKNLsawBUUOmik', 'IMveCViyAvmoTEQqmbcTbdfjULnnl', 'NRvsdotmmfwumsDpDtZU', 'mnqVnwWOvMiD', 'HXpHrMvGQpbuhVgnUkfFPqjpoRdhXBrFB', 'awtr', 'IMIdOmMHZccbOZHhWOKcKjkwwgkJSfxHDCzR', 'jPLISbIwWJEKPwgvajTxVLws', 'HBfRrzEC', 'VXsysGnAsFbqNOvIaR', 'upCaeaIOK', 'GUDFkrzBiqrbZVnS', 'MoCOePXRlVqCQpSCaIKpEXkH', 'rfF', 'fjhMEpySIpevBVWLOpqi', 'KdeskLSktU', 'vjUuNUlBEGkQyRuojZLyrmf', 'SvSxotkTKCeVzNICcSZLsScKsf']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('gcDqqBCNqhQgVVgsxMXkevYIAxNl'), ['BHnoKRqOoKgmOVkjtehGSsInDvavDWOhkKAUL', 'nYqpmKPTWGdnyMcg', 'TIplHzsSXUz', 'SiQwpQgEdZ', 'YoJTWBJgsbJvq', 'CwyazvXERUFMCJWhTjvltxFBkkvMwAysRLe', 'tXUxqmPbYFeLUlNrNlvKFKAwLhCXg', 'vUbNusJGlwsOyAqxPS', 'ME', 'ASUzpELipnYwAknh', 'VtTdMpsQALpibryKQfPBzDFNLz', 'KmujbORrULAYfSBDyYvA', 'BaLGNBliWdgmqnzUx', 'IzwKIbbSUiwhFQrujMgRcigX', 'pnS', 'UKSZbRGwGtFyLMSxcinKvBvaX']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('HCPOPUUEVVsuZDbyRnbowGuOMhQ'), ['UzDbYrNBoWgUo', '', 'pUUEVVsUzdByrNB', 'nBO', 'SUZdbYrNbOWgUoMH', 'pOpuUevVSUZDbYRnb', 'bowGUoMh', 'VsUZDbyrNbo', 'suzdBYrN', 'uueVvsUZDBYRnBoW', 'gUom', 'eVvsuzDBYRNBoWgUOM']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('RIDPJWYYSGBFWyXikHofbTcZAnj'), ['aFxQyVe', 'OcnZBgPsA', 'iBQaH', 'oesSvsWtgQprSSIPaDHdW', 'EfytiMfW', 'qHiFjeUvQRm', 'LfQkfmhTMUfoTOmGJUnJpevIoPpfpzMuKKjv', 'scYbCYNzJhEMMg', 'yTLwClSbqklywqDiSKmEdyfU', 'HYlGFMM', 'TMQhjOMTImXbCv', 'AVtzpxurFkmpVkddQANedlyVlQsCXWcRjEr']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('GEsmYgXgMWWYsdhZaVvikXZiN'), ['wySd', 'smYgxGMWWYsDHZ', 'vIk', 'smyGxgmwWysDHzAvvikxZi', 'WYsdHZAvVI', 'YGxGmwWYSDhzavvI', 'XzI', 'ySDhZAvvIK', '', 'myGXgmwWySdHz', 'MYGxgmwWySdHZaVvik', 'wYsDhzAvvikXz', 'wwYsdHzav', 'Z']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('XKCeCpxYeaYOWzIDcreyPWJWdrck'), ['tTRLUYJTkSWOabLJlIBshARIkwVRKemt', 'jQgn', 'wdNRsKIVunGlvwqkwn', 'BsbKGBJlkWQDBwqqeIjENvtkQue', 'yLuUru', 'zoLGzThznNmsitmJFIjQ', 'WFKnfdrnoxOWcXBqxkvqrFbahQx', 'QHbgRXcfuESPcMkwGJuDN', 'NPqfqLS', 'bi', 'HnccYFPObXjeGYtrmAEHDZQiXTvbNcOiesqRPS', 'KobVCJewfUsjBXDfgSnPxzeJhz', 'AqYNUPOYDZjwXx', 'xbZydBGZFFYFsFHwm']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('AnIhBNnXKYQwRSuSqrDCnI'), ['', 'HBNNxkyqWRS', 'xKyqwrSUSQR', 'yQwr', 'ihbnnxKYQWrsUS', 'bnnXkYqwrSuS', 'qWRs', 'nXKyqWRSUS', 'qrdcN', 'NiHBnNXkYQWrS', 'NnXkYQwRSUsqRDCn', 'rSusqRd']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('OySHBUpomaqcWHcHgyufm'), ['lihJlyBiOyyqzeveErImIJuJlfl', 'WyfAXSwZPcxOEDtiCGBJvkCHNnYfA', 'hZ', 'fDQzngAutwHSVeoGVihUyvHXmAE', 'aCpcZqWKdNqTdLwBnQENgQptIyRuOT', 'PFQVrlctEwb', 'ggpNUNnWqoubvmAFdjhLXzohmT', 'VFsfaLwcwNME', 'nHuIzNMciJjmK', 'OryyjtFfIaxViPXRyzKiMu', 'XufDMKXzqKjYynmmZzZHcDm', 'xWbDgq', 'ArElRZqdLQmN', 'obzvBzKQuJXZHMVmEBgFdnnQvtZSV', 'ZEHSnSmlbfsjc', 'gjmWPiLylEkYMTFCOVFB']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('NwMuwbdjhSYlzKoAZIceDx'), ['ZKOaZ', 'wBDJhsYlZKo', 'hSy', 'MUwbDjHsyl', 'sYlzK', 'ylZKOAZ', 'y', 'lZKoaZICEdX', 'azIce', 'djHSylZkoAzice', 'djHsYLZKoAzi', 'dJHSYlZK', 'muWbDJHsYLzKOaziC', 'zi']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('gtBXzVqRbepHJVsMocOxn'), ['DidFXiqhRVBCHBVklLHudA', 'yEhumIpaYXlj', 'iaEmViTRLPM', 'vTwKBlbpaJZGYGdMifOVd', 'zvgfzWeLsMQNLutdAdCeuAgEBhy', 'Ca', 'iHabiaRoIeiJgSx', 'EBfgrJnzHbuinysDBKc', 'kT', 'SGIT', 'BTRuKgHDuXMzxwwEgvE', 'OWJIeTLqLfaPT', 'BQM', 'yMimBqutKovoBIvMBok', 'zIBCYVNYAwu', 'EFDEFWGqvuxygsLszSwSiWYEqJu', 'QJDIXvPOYtvhPyfIKqebhTfL', 'ssALaXRxjguUIVKMCdWRPkivww']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('MowjvqBkjnVTelCcXpoSuUowuzF'), ['Su', 'vqBkJNvTelC', 'Elccxp', 'vtElc', 'JVqBkJnVTELCcxpOsU', 'OsUuOWUz', 'ElccxPoSU', 'wJVQbkJNVtElCC', 'xpOSUUo', 'VQbkJnvTELCCXp', '', 'TeLcCxPOsuuO']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('VfVQmlYIDdGBpRyfoeuLffUUpMordC'), ['vqMLyIddgBPrYFoEulFFu', 'lyIDdgBPrYFOeul', 'dGBPRYFOeUlffUupmOrD', 'OEulffU', 'pMordc', 'FVqmlyiDdgBpRyFoeUlFfuUpMOrD', 'PmO', 'o', 'YiDDgbPRYFOe', 'DGBPryfoeU', 'yIDdgbpRyFOeULfFU', 'lyIddgBPryfoeulfFuU', 'gbPrYfOeUlFfuupmO', 'yFoeULF']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('CdnrzjzmwtMMPLjgcXWsbtrBs'), ['RfgIUeerlPIozKpRQR', 'QRoYzjZlgngJxX', 'mEbqlBIzTQH', 'UmrfJxKyTllktPfyHA', 'ukoZeOPA', 'pbbRaUcJijcxt', 'Rg', 'lSBG', 'HvuwuiqVy', 'Fo', 'aGpUVjaFCrOwFCvjc', 'zKhfkgymcWmXdsSrqAHBnxJhvcpplgUecg', 'ioTdwUnrJBGUEESnxKuaRM', 'QciYRCjRDUxPkafN']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('miTQkQcxbKMwGOyzzRJpfXLyGx'), ['yMwgQQJkeshUugm', 'wGVe', 'XncShWqjp', 'KWjGQCOsfMKWRcgCfebkXZwZ', 'SFWbU', 'WdFDMIcfWeApTteNfcDsHIjEB', 'XRuUJznPOCQbK', 'tibBMGZHiIKVAKuUAIwuRAAfG', 'VVCqVGGObZLQsuqUjrXrsBSQJKChGpZxb', 'bWYAOLuwMcwWYeECkpVYLGeWHRrIp', 'SLzCgfkRWmZQQcQzP', 'VvfOhFBhfiVezUSPdIbr']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('KXoTIgVktxiXoEwfoLCENiEhz'), ['oLCENie', 'xix', 'en', 'IgvktxIXoEWFOLCEnieHz', 'xOEWFoL', 'LC', 'ktxIxoEwfolCenie', 'ce', 'oTIGvktXIXOE', 'eW', 'otigVKTXIXOEwFolC', 'E', 'CEni', 'gVKtxIxoEwfOLCENieh']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('DXKzSivrdLuBdCrEYfMEgPhOZ'), ['', 'sIVRDlUBdcr', 'luBDcrE', 'rDLUbDCreY', 'KzSiVRdLuBDCr', 'dcREYFme', 'lUbdCReyFMEgph', 'sivrDlubdCr', 'BdcreYfMEgP', 'ZSiVrdluBDCrEYfmegpHOZ']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('lTvINMXVojkokvNBXPZOm'), ['ZQOJMEJfrjm', 'vIpmXnGlmWze', 'wbdDKcjrrIzBHypzJU', 'omotHOYbZjWfyVNeNtyOsfXPALJG', 'SXxu', 'yZPDFsZq', 'OVYVWUjQDSQTKRgKoHSovXbROLRQ', 'RnXWZfZwHipewOJimTeRoNRYIdcZGzv', 'sizoEJibbfzwqFb', 'vgFmePQYlajiqSyBpvaKdmMYZohM', 'ENsFoFCxDQofsBSkLZRtOcJNU', 'nG']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('LsTqxiGRdvQClVNBCGMOUHOAmOqPEC'), ['NdFuUQEUWaxS', 'fdOHzUzineBDnWJJvhPNZgB', 'rYAWGIBPxOLrjuquqGjLLoIHrHqSFmjh', 'IVgYBJARY', 'ToivVgUJAxRJoCIFo', 'yQXGrRjhIqFtC', 'PNYdEPsWVqjZOhanGNAq', 'nrQIDDOfETr', 'usJcPtiHKhgKtYO', 'vPKqumGhPbmAJGAoiyZHJvNBd', 'eXINlP', 'WQeESQJcJJV']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitive(materialize('gRzzQYOwLNiDcMFjXzSFleV'), ['XZSfLe', 'wLnIdcMFjxZSf', 'F', 'm', 'Le', 'qYoWLNidcMFjXzsf', 'zqyoWlNIdcMFj', '', 'oWlnIDCMfJxzsfL', 'wlNIdCmfjXzS']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitive(materialize('cYnMXJMJCdibMXoUQHEw'), ['BFrGFZRgzwHGkUVbBiZMe', 'piORdVIWHMBsBDeJRLbGZAHGBrzNg', 'bmDePbTPnFQiCFfBJUxAEYNSbgrOoM', 'gtzeAGwqjFrasTQUgAscfcangexE', 'okLG', 'l', 'EBkkGYNZZURgFgJPlb', 'HDQVngp', 'vEHhtBqWhZHCOrqEKO', 'fgqdFc', 'COig', 'VftTpSXAmTmvnShHJqJTdEFcyKPUN', 'WDI', 'knBm']) from system.numbers limit 10;

select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('мтдчЛВЖАгвзщущвкфИКмТбжВ'), ['щУщвкФИкМ', 'чЛвжАГвЗЩуЩвКФикм', 'ДчлвЖАГвзЩУЩвКфИКМтБЖВ', 'ЖагвзщуЩВКФикМТБжВ', 'ВжагВзЩУ', 'гВЗщущвкфИКмТБж', 'ГвЗщ', 'щВкФикМТБЖВ', 'вЖАГВзщущ', 'взЩуЩвКФИкМТ', 'ЧЛВЖагвЗщуЩВк', 'тДчлвЖагвзЩуЩвкфИк', 'ТДЧлвжаГВзЩущВ', 'тДчлВжАГВЗЩУ']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('дтрцФхИнпиОШфдАгзктвбУвсб'), ['чТрВиУРФсРпДЩОащчзЦНцхИДА', 'ЗжмПВтмиойУГхАЦПиДУЦноНуййЩХаФТофшЩ', 'уБшлОЙцМПгетЖЧетШжу', 'ЧзИАУХобФрачТеХОШбМщЖСамиМВАКРщАЦ', 'ВйвТзхЙФЧоАЖвщиушАз', 'ЦшИфххкжиФйСЛЛНЛчВоЙВПпхиИ', 'ОатЕтщкЦпбСБйцОшГШРОшхБцщЙЧиУЩЕеФлщ', 'цСПпЧА', 'ШЧНфПмФсКМКДВЦАоФчОУеТЦИзЦ', 'зАбдЛНДГИ', 'фхЩлЗДНСсКЖИФлУАбЛеТФЕпЖлпПхЙиТЕ', 'иВшкНслТКМШЗиДПйфвйНкМЛхеФДзИм', 'лпушПБванпцев', 'ЧОшЧЧмшЦЛЙйГСДФйЛАв']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('нщМаНдЧЛмиВврПокПШмКБичкхвРГ'), ['АЙбаЙйШЛЙРЦмЗчВеИЕощсЦ', 'щЦФдВжчТСЩВКЦСпачЙсумщАтЩувеиниХПДоМС', 'иоАкДРршуойиЩищпрфВаЦПж', 'еЖПйШкГжЧтоГЙМВ', 'ЩПалиБ', 'ТвВлт', 'оХжйЛФеКчхЗВвЕ', 'ерцЩ', 'ШХЖОАрзеп', 'ККМрфктКГишпГЩхаллхДиВИИЛЗДеКйХмжШ']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('вШЙчоМгОттЧАЕнЧаВеЦщчЧошМУ'), ['ЧОмГотТчАЕН', 'ОмГотТчАЕнчАвецЩчч', 'ЧАВецЩч', 'ТЧАеНЧаВ', 'ттчаЕнча', 'ТчАЕ', 'мготтЧАенчавЕЦЩ', 'НЧаВец', 'тТЧаенчАвецщчЧошм', 'Ав', 'ТЧаЕнчавецщчЧоШму', 'аЕнЧав', 'АеНЧав', 'шйЧомГОТТчаЕнчАВЕ', 'шйчоМгОтТЧаЕНчаВеЦщЧчош', 'МУ', 'ошМ', 'гОТтЧаеНЧА']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('фйадзЧмщЖШйЖЛшцГигцШ'), ['НТХеМРшДНУЗгадцуЧИ', 'жпСИКЩМлНлиоктлЦИвНЛ', 'КхшКРчХ', 'кгТЗаШИарХЧЛЖмСЖм', 'ОмиЛй', 'жЕРбФЩНуЕКЕАВоБМОнАЕнКщшзйПкОЗ', 'гиЗдадкбжХМЗслшВИШай', 'двтЗйЙНгПуТзД', 'ТНкмаВЕФ', 'Шеа']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('ШЕшхмеЦХеАСКощеКИфлсТЧИЗЛ'), ['КифЛсТ', 'ХеаСКощЕк', 'КифлсТЧ', 'шХМеЦхЕаскОЩеКИ', 'ЕшхмЕцХеаСК', 'ХЕасКоЩ', 'чИ', 'ЕцхеАсКОЩек', 'ЩЕкИфлс', 'асКощЕкифЛсТ']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('шоКнВЕрОЖЛпУйХзСугКПВжиРсЙпо'), ['игВербфНахчжЙггч', 'лтимрдфЕг', 'нкеаЖАшНБвйСдКИВГДшАГиАТнФШ', 'МжсТЙМГОииУКВГнцткДнцсоАд', 'ХтпгУСдБдцАЖЛАННоЕцзЕшштккз', 'ншУЦгФСЖшмс', 'нЩшМ', 'гоЖхМшаЕмаДРЧБЛИТпмЗОоД', 'фГКШхчФбЕГЛйкчПИЙххуМГНШзхг', 'ХпХщПЦАзщтг']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('кЧбоЗХфвБХхусмШгНаШШаГзш'), ['Ури', 'лММшткфНзцЦСВАдЩПМШфйОМшефигЖлуЕП', 'сМтЕдчЦафйСТЖЗфлРЙПЦдипжШскцВКХЦЖ', 'АУкжИФцшЛБЦЧм', 'ФПлнАаДСХзфоХПСБоСгМТОкЗЧйЛ', 'ЦшСГЛрцДмнНнХщивППттжв', 'жзЕгнциФ', 'МШЛсЙЧтЛАГжд', 'уИиЕжцоРНх', 'ЧбйГуХтшОНкрЧИеПД', 'ЦдЩЕкКвРЦжщЧциекЗРйхрббЖуЧ', 'иВжен', 'ГчОржвБГсжштРЕБ', 'ШоЖдуЙфчсЧегумщс', 'йчЙГ', 'РДедвТ']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('ткРНбЩаРкгГчХшецИкНЕнСЖкйзАуУЖ'), ['ХлЖхУИллрРННйЗйсРуШЧвМбЧЧщфФЦц', 'СЛчКБцСФДшлфщаФлЙСзШабмбхуБжТСТ', 'УКУиввЗЩуВМцпчбпнДГбпЕЖрПбИДркМРОеЧмЧдГ', 'ПчщвШЩвГсЛмММГБ', 'хКЦЧсчжХЩИЖХеНнтоФЦлнмЛЧРФКпмСшгСЧДБ', 'удсЗйУДНЧУнтЕйЦЗЖзВСх', 'хПЖЙИрцхмУкКоСмГсвПаДОаЦНЖПп', 'сВОей', 'ЩЦжщоабнСгдчрХнЩиМХзжЩмФцррвД', 'ЦИсйнЦДоЕДглЕЦД', 'жзйПфБфУФоцзмКЩГПЧХхщщПТпдодмап', 'ДНХГНипжШлСхХхСнШЩЛИснУйЧЩЖДССФфиС', 'ОйЩНнйЕшцФчБГЛвхЖ', 'КЧРВшИуШйВфрпБНМсУмнСЦРпхЗАщЗУСвЧйБХтшХЧ', 'зЛбНу', 'ЗСрзпшЕйРржПСсФсШиМдйМЦГхдйтРКЩКНцкбмгС', 'СУццБуКнчОищГ', 'уЕГЧлЗБНпУисЕЛ']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('ВЦХсЖЗЧЙБЗНбРитщстеМНжвВ'), ['итщст', 'ЧйБЗНбрИтщстЕМнЖ', 'ХСЖЗЧйбзНБриТщ', 'Темнж', 'сЖзЧЙБзнб', 'хСжЗчйБзнБрИтЩстЕм', 'БзнБРиТщ', 'ЗчЙбзНбрИТщ', 'чйбЗНбри', 'зЧйбзНБРИ', 'нБРитщсТе', 'зНб', 'цхСжзчйБЗнБРИТЩСтЕм', 'жЗЧЙБЗнбрит']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('ХцМШКАБАОххЕижгГХЩГиНциД'), ['ОРАБЕРВомЛфГНМИКупбхЛаАкЗдМзтш', 'лЗУЩнлбмиЛАфсгМРкцВтлснййишИНАС', 'ТлжлУоУгжукФжЖва', 'жоСШПоУНЩшРМГшОЛзЦБЛиЛдТхПДнфжн', 'чнСУЗбДаГогжДфвШКеЙПБПутрРпсалцоБ', 'ЙозоПщчакщаАлРХбЦгац', 'иаИСсчЙЧБШорлгЧТнчцйзоВБХбхЙФтоЩ', 'ПСзсБЗЕщурфДЛХйГИеПНрмииаРнвСФч', 'ЦйЖЕуТфЖбхЩМтйсЙОгЛбхгтКЕЩСАЩ', 'гтЗуЩлужДУцФВПЛмрБТсСНпА', 'тГвлбчЗМасМЖхдЕгхмЩксоЩдрквук', 'ВРаг']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('тУйВЖдНнщцЗЖфКгфжГфиХСБЕЩ'), ['КгФЖГФи', 'сБе', 'ЖФ', 'гфжгФИхсбе', 'ВЖДНнщЦзжфКГфЖгфИхсбещ', 'ВЖДНнЩЦзжфкГ', 'вЖДННЩЦзжФКГфЖгФ', 'ф', 'НщЦЗж', 'нщЦЗЖФк', 'Их', 'дННщцзЖФКгф', '', 'нщцзжФкг']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('ШкКРаоПеЗалРсТОиовРжгЙЧМКЛШ'), ['рчсажЕК', 'пЧТМфУрУММждЛйжзУрбкмам', 'бАШеНмВШзлзтушШШсхОсцрчЙПКИБнКжфЧЕХ', 'ЖМЛшбсУМкшфзочщАЖцМбмШСбВб', 'гтРХсщхАИОащчлИЧуйиСпСДФПбРл', 'ЧуОРУаоойГбУппМйЩФДКПВ', 'уУпугйРЕетвцБес', 'ЙЖЦТбСЖж', 'ИБКЛ', 'ТДтвОШСХГКУИПСмФМтНМзвбЦрднлхвДРсРФ', 'вВгНЙХИрвйЕЗпчРГЩ', 'ПчмТуивШб']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('РлчгхзуВШежХЦуМмнВЙщдцО'), ['ХшвМЦДШпЩОСшЦПдруа', 'ФИЦчУвРкпнПшИЕСЧАувиХд', 'фшвбЦОИЗфпИУМщзОЧЗфВцЙПнмтаТгг', 'мЖЩйавтнМСЛ', 'НВбШ', 'ааФДДрВвЙТдПд', 'ЗнчЧущшхЙС', 'рзуСзнеДфЩПуХЙЕл', 'ШСЩсАгдЦбНиШмшКрКс', 'ггнЕфБГзрОнАГЙзЧеИП', 'вшТИпЧдЖРкМНшзпиоиЩчзДмлШКТдпЦчж', 'фЦТЙц', 'ОтУшмбптТКЗеПлЧцЛОкЩБпккфгИн', 'ЩпвхпЗлШБЦ']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('ЙбйнхНщЧЖщчГОАпчФнЛШФбгЛа'), ['щчг', '', 'апЧфНЛШфб', 'ЙнхНЩЧЖщчгОАПЧф', 'ХНщЧжЩЧгоАпч', 'ХНщЧжщчГо', 'нщЧжщчГОа', 'чЖЩЧГоапЧФНл', 'оапчФ', 'щЧГОАпЧФНлшФ', 'ЩЧГОАпЧФНЛшфБг', 'БЙНхнщчЖщчГоаПЧФНЛШФБгЛ', 'ОапЧфн', 'ф', 'БглА', 'ш', 'шфбГ', 'ХнЩЧЖщчГоА', 'ХНщчжщЧгоапч', 'хНЩчжщЧГоапчфнлшФбгЛ']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('кдЙДТЩеВЕфйКЩЦДиКБМф'), ['щфЛ', 'фЧЩЩичрКйЦКхеИИАпоБВЙЗбДАФио', 'мИтиЦРоВЙсБбСлНзиЛЧОфФевТмижщК', 'тЙгнКШфНТЕБЛцтГШЦхШхБ', 'уаабРГрМЙпМаБуЗпБЙчНивЦеДК', 'мпВЛНДеКПУгРЛЛинзуЕщиВШ', 'ЩжКйШшпгллщУ', 'пршЙПцхХЗжБС', 'нбЗНЙШБш', 'йцхИщиоцаМРсвнНфКБекзЛкчТ', 'хсмЦмнТрЩкДТЖиХщцкЦМх', 'ГмЛАбМщЗцЦйаОНвзуЗмЕКПБЙмАЕЛГ', 'ОЦХРЗРмкжмРИЖИЙ', 'з', 'лЕТкпкдЗчЗшжНфо', 'ИТПфйгЖЛзУТсЩ', 'ОфрбЛпГА', 'МЖооШпЦмсуГцАвМЕ']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('ЩГТРШКИОРБРеНЖПКиуМОкхЛугИе'), ['брЕнЖ', 'РбрЕНЖпКиУМокХЛу', 'ГТрШКИорБРеНЖпКиУМ', 'рШКиоРбрЕнЖпкИУМОК', 'ИорбрЕнЖПК', 'Окхл', 'шкИоРБРеНЖПк', 'ТРШкИоРБрЕнжПКИУМОкхл', 'КИОРБРЕнжпкиУм', 'Н', 'КиОРбРЕнЖпкИУмоКхл', 'к', 'ГтРшКИоРБРЕнЖпк', 'гтрШкиорбрЕНЖпк']) from system.numbers limit 10;
select 0 = multiSearchAnyCaseInsensitiveUTF8(materialize('ШНвпкфЗвгДжУЙГлрТШаШЛгНЗг'), ['нЗБенВшщрЛАрблцщшБАдзччммсцКЖ', 'бЗЩхзЗЗбФЕйМоазщугБбмМ', 'рЙсВжВсхдйлЩгБтХлчсщФ', 'пиБшКРнбВБгЕуЖ', 'жПшнхпШзУБрУЛРНЩДиаГШщКдЕвшоуПС', 'чЕщкЗмДуузуСдддзгКлИнгРмЙщВКТчхзЗЛ', 'кЖУЗЖС', 'щххОВМшуажвН', 'фбцЖМ', 'ДШитЧЩДсйНбдШеООУдг', 'ЛХПфБВХЦТИаФПЕвгкпкпщлхмЙхГбц', 'чЦсщЗщрМ']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('ФРХгаСлчЧОцкШгзмКЗшФфББвЧ'), ['кзШфФб', 'ГАслЧЧОцкшг', 'ФфббВЧ', 'ЦкШ', '', 'АслчЧОЦКШгзМкЗШффбБвч', 'РХгаслЧчОЦКШГз', 'РхгаслчЧОцКШгзМкзшФфБбВ', 'Шг', 'Ф', 'ХГАслчЧоцКШГзМкзш', 'ШгЗмКЗшфФб']) from system.numbers limit 10;
select 1 = multiSearchAnyCaseInsensitiveUTF8(materialize('ЧдйШкхОлалщНйбССХКаФзОМрКЕЙР'), ['бссХкафзОм', 'ХОЛаЛщнйБссХкаФз', 'лаЛщнйБсСХ', 'ЩнЙбСсхКаФЗО', 'йБСсХКАФЗОмР', 'йшкХолаЛЩНйбсСхК', 'С', '', 'ЙшкхОлалщНЙБсСхКаФзом', 'Йр', 'щнЙБссхКАфзоМрК', 'рКе']) from system.numbers limit 10;


select 1 = multiSearchFirstIndex(materialize('alhpvldsiwsydwhfdasqju'), ['sydwh', 'dwh', 'dwhfdasqj', 'w', 'briozrtpq', 'fdasq', 'lnuvpuxdhhuxjbolw', 'vldsiws', 'dasqju', 'uancllygwoifwnnp', 'wfxputfnen', 'hzaclvjumecnmweungz']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('kcwchxxyujbhrxkxgnomg'), ['jmvqipszutxfnhdfaxqwoxcz', 'nrgzkbsakdtdiiyphozjoauyughyvlz', 'qbszx', 'sllthykcnttqecpequommemygee', 'bvsbdiufrrrjxaxzxgbd', 'hdkpcmpdyjildw', 'frxkyukiywngfcxfzwkcun', 'dmvxf', 'esamivybor', 'eoggdynqwlnlxr']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('zggbeilrfpkleafjjldgyfgn'), ['rpypxkpgvljhqthneremvabcd', 'qchzlnsctuwkdxqcrjgihvtfxhqxfqsxm', 'vtozkivjyqcqetmqenuihq', 'fixcvjyzbzejmwdivjf', 'lydoolvnuuamwlnzbyuuwpqqjaxf', 'elkodwthxqpcybwezm', 'wpiju', 'wdzuuwumlqfvga', 'iokphkai', 'wkbwdstplhivjyk', 'wxfbhfturuqoymwklohawgwltptytc', 'jehprkzofqvurepbvuwdqj']) from system.numbers limit 10;
select 9 = multiSearchFirstIndex(materialize('bwhfigqufrbwsrnnkjdzjhplfck'), ['v', 'ovusuizkdn', 'ttnsliwvxbvck', 'uh', 'lfourtjqblwdtvbgtbejkygkdurerqqdwm', 'snmtctvqmyyqiz', 'ckpixecvternrg', 'gluetlfyforxcygqnj', 'igqufrbwsr', 'om', 'huwazltjsnohsrcbfttzwyvcrobdixsuerkle', 'gqufrbwsrnnkjdzj', 'hfigqufrbwsrn', 'lhhyosbtznyeqzsddnqkfxayiyyajggxb', 'igqufrbwsrnnkjdzjhplf', 'pl', 'jtbqaqakbkesnazbvlaaojppxlbxccs', 'gqufrbwsrnnkjdz']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('yevfiumtjatfdnqixatbprvzwqlfgu'), ['ozghvskaixje', 'vmdrvdjhwxdvajmkpcxigsjzmtuhdxgllhzrpqd', 'qfhnxpcmtzpociajidwlcvobjfyxfcugsxy', 'pgamvhedjibcghinjrnowqzkfzibmfmh', 'bcmrdzpcczhquy', 'czosacvwfsbdvwwyirpvbve', 'qu', 'fdkobwlnmxbpvjkapextlbcrny', 'bqutjqobkyobhtpevjvewyksnoqyjunnnmtocr', 'kjlgff', 'oitltmhdburybwfxrjtxdiry', 'kiokuquyllpeagxygqugfmtm', 'wlbkl', 'khubpmstqjzzjzmsvfmrbmknykszqvue', 'lqrbmyndsztyrkcgqxcsnsanqjigimaxce', 'nitnyonuzedorrtkxhhgedohqcojbvtvjx']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('wmvuoeqphsycrvtxghrcozortmdnh'), ['hv', 'ugcmpebvlzgdtcmgkbgzyfel', 'qvmofayljsvybupvvnbhhibsz', 'zvlihxmyxlxwbffwjzjrfjgimmltftqqre', 'mwassqvxptav', 'jrumvqzkiaewngoufhrleakcfrsaxhpxyg', 'sxlxwhvkpavgfhxrxcbnqbstyrejtosxwe', 'psnlqakyfhcupryqatrmwqlswwjylpaiqammx', 'ivozojwldsgtnxpvsi', 'epyzjs', 'legi', 'sdqxxahfbddhacqrglgdcmlslraxfaahhfyodon']) from system.numbers limit 10;
select 12 = multiSearchFirstIndex(materialize('lebwdwxfdzwquhqhbvmte'), ['mwhruilzxvlyrgxivavxbbsq', 'ubuiizuasp', 'xpkzcsf', 'qpeqitoqqqeivohajzhmjbo', 'kbftixqmqgonemmbfpazcvf', 'iyhluioqs', 'hws', 'tupfdksgc', 'ows', 'pngzkoedabstewcdtdc', 'zdmyczldeftgdlwedcjfcoqycjcivf', '', 'xt', 'syuojejhbblohzwvjzzedzgmwc']) from system.numbers limit 10;
select 7 = multiSearchFirstIndex(materialize('wcrqaoecjwkhnskrbahqxfqgf'), ['qegldkdmyaznlmlhzvxfgoukngzbatnuq', 'khgcvgrifwtc', 'hkwcpogbbdqulizrycmneqmqynvj', 'zkqjf', 'xfduxyy', 'ructdekcoywfxsvpumfefoglljptsuwd', 'wkhnskrbahq', 'crqaoecjwkh', 'ikmpbunpguleinptzfelysiqc', 'lhldcci', 'nooepfypkoxxbriztycqam', 'uxeroptbiqrjartlnxzhhnlvjp']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('psgkkcwttitgrjsobiofheyohadu'), ['achfrepey', 'minlzeiwgjfvvmhnevisky', 'oxfghfdthtyczzveppcoxrued', 'ydhaupodnezvxhcqahfkwtpvxnymriixf', 'slxsbxidylxyurq', 'socyyabwbjdabnuqswrtjtqogirctqsk', 'lvbnacirctyxxspjmispi', 'oj', 'ihmmuuqlosorrwhfxvpygfrzsqpmilcvjodmcz', 'idmtmemqfyrlbwhxz', 'hsqfsfdzvslwbtlwrfavez', 'gszl', 'ei', 'pnywjnezncpjtyazuudpaxulyv', 'iqgavdjfqmxufapuziwwzkdmovdprlhfpl', 'yigk', 'mjidozklrpedutllijluv', 'vixwko']) from system.numbers limit 10;
select 3 = multiSearchFirstIndex(materialize('xtjxvytsseiqrpkbspwipjns'), ['bwmoghrdbaeybrmsnucbd', 'zoslqabihtlcqatlczbf', 'sseiqrpkbspwipjn', 'mdnbzcvtayycqfbycwum', 'npueimpsprhfdfnbtyzcogqsb', 'ytsseiqrpkbspwipj', 'fzvhcobygkwqohwutfyauwocwid', 'naacyhhkirpqlywrrpforhkcjrjsnz', 'vezbzderculzpmsehxqrkoihfoziaxhghh', 'mvvdfqzskcyomjbaxjfrtmbduvm', 'pwipjns', 'tsseiqrpkbspwipjn', 'sseiqrpkbspwip', 'qgrtbcdqcbybzevizw', 'isjouwql', 'rlbeidykltcyopzsfstukduxabothywwbq']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('zxmeusmehplcgbqabjof'), ['hqxgrw', 'fydjyrr', 'cocwtbazwjrswygttvrna', 'wpkvowuq', 'mwnzdxihrxihzhqtl', 'ljkjtmrfbonhqkioyzotyeegrw', 'ofxo', 'rjubwtpbweratrelqlrqotl', 'wvxkcil', 'qvolxxgqs', 'afqlhjnlvxowtnuuzywxuob', 'slwbmq']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('tjcmtoisgbilkygushkpuxklis'), ['bkdohwx', 'dfohgzhcjqirlbrokwy', 'zaemgqgxltznvkccyumhgsftnfigbol', 'otgcaybejwe', 'qn', 'gvfzcyhvmsnbgkulsqrzeekmjkc', 'cajuyauvmhkrriehgwfmtqbkupysudle', 'pmcupysyllzpstolkfpdvieffxaupqtjty', 'elhlzvescbfpayngnnalzixxgunqdhx', 'cvxpgdnqcxeesk', 'etlewyipypeiiowuoewulkpalvcfe', 'ordhwrkwqq', 'wnroixlkrqnydblfrtlbywc', 'xshujuttvcdxzbetuvifiqi', 'meqqxqhntkvzwoptnwskdgsxsgjdawe', 'dnmicrfshqnzosxhnrftxxeifoqlnfdhheg']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('iepqqbvekaflprupsmnpoijrld'), ['kqomoeysekwcplpegdwcdoeh', 'mwdvr', 'aobviioktzwzmpilblbdwstndhimabfgct', 'vqustluciruiyfkoontehnwylnauwpol', 'utcqnitztcgr', 'ityszrqmlwzspnrwdcvdhtziob', 'hmll', 'ilfzvuxbkyppwejtp', 'euxdzqcqutnfeiivw', 'rbcjlmjniiznzaktsuawnfjzqjri', 'fzyxlzzretsshklrkwru', 'jrujmdevqqojloz']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('cufztqffwjhtlkysekklpaywemm'), ['cpawuauqodogaitybtvplknjrsb', 'ynsocxfnxshzwnhlrfilynvz', 'ylrpytgcvtiumdckm', 'mvgrkueaslpgnjvvhzairgldtl', 'iliorsjypskmxfuuplfagktoycywb', 'drvwngp', 'zviuhcxaspwmqqz', 'qfgmrmhycskus', 'szj', 'rooivliiqufztcqlhrqyqvp', 'tufdmsmwue', 'cssowtldgwksbzlqyfereodcpuedighwd', 'odcjdffchhabtaxjvnr', 'o']) from system.numbers limit 10;
select 7 = multiSearchFirstIndex(materialize('zqwvlarwmhhtjjgwrivwfpsjkvx'), ['zcwhagxehtswbdkey', 'okezglmrjoim', 'ilwdviqimijzgoopmxdswouh', 'aqztpsntwjqpluygrvwdyz', 'uzxhjuhiwpz', 'akgc', 'larwmhhtjjgwrivwfpsj', 'isqghxsmcrwlgyloslmlyeboywtttgejdyma', 'arwmhhtjjgwri', 'rwmhhtjj']) from system.numbers limit 10;
select 9 = multiSearchFirstIndex(materialize('fuddujwwcewlhthgwsrn'), ['shtzrrtukxmdovtixf', 'rkcnzzzojqvvysm', 'jlamctgphjqcxlvmpzyxtghnoaq', 'pthrwvbheydmrot', 'kpniaqbcrgtxdyxxdxonbbltbdo', 'igulngxgtauumhckvbdt', 'khgrmskijoxruzzzaigjxonsc', 'rxzeykfxwssltw', 'hthg', '']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('jtgvvkggpkqhbxptjgoy'), ['nplzawmacgtqfxsp', 'oosw', 'akw', 'hnsenqoqwiydiufozomkyirgjepeqw', 'fpafgahvfdxukzvskbuy', 'tqimmsqffiqfoni', 'rrxkjklmkdhxqwcpfyutqzxu', 'esfqeujcbqxwnvodkwwdbsyozptaf', 'rqnyguyz', 'fftl', 'ccfyavxtxrpi', 'wftpsblszgovfgf']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('steccxkwnptybaddcuau'), ['qagxfznhjaxtyclxdsi', 'rtxwptfyzgthkwrx', 'rmcoxxs', 'vlubx', 'siecygstzivz', 'tksiagm', 'kq', 'dgsqrobxegmdbjkanb', 'lxokyvhveklvdakrxyiqokr', 'tgpmehwdrirpfjonqzhqshbo', 'cqmkargvsfjoxrguymtzsfwkg', 'avkmufhoywprjw', 'xzywtvlpoozmgkrcavevwebv', 'hfiuwslapamiceaouznxm', 'tmfjhqddafhhjbybfphlbwu', 'mrigvhmjvdpny']) from system.numbers limit 10;
select 0 = multiSearchFirstIndex(materialize('ccbgxzoivbqtmyzqyooyepnmwufizz'), ['lcclseplkhxbrrzlnani', 'xggxivwqlpxmpypzovprdkmhrcgjkro', 'dbbmiegotfxjxybs', 'hqtcowpupsyqfx', 'znatfzjbeevbaqbmpofhywbyfxn', 'mnditiygex', 'lazqapwjswhkuimwmjoyseyucllnrfxrwnzj', 'jg', 'dmqwnuvsufgffuubhqeugwcanvflseorrydyyxvr', 'wpjfcfwfgjiybncrw', 'joucnvxxcyjyqlwhrzwnstyj', 'babtxkzasyaffxzd', 'wgcfdyhwxjoytbxffdxbdfinolbltnhqkvyzybc', 'yhrgwbdwopznltjtyidxawqg', 'bvrrt', 'bcwmsys', 'ijdjojhhzaiyjyai', 'eevxwppogogdbmqpbeqtembiqxeiwf']) from system.numbers limit 10;
select 2 = multiSearchFirstIndex(materialize('xrwjeznohtbdvijwsbdksf'), ['hwdfufmoemohatqafdrcvdk', 'tbdvijwsbdks', 'xzwjczbuteujfjifzkbxvezs', 'bdvijwsbd', 'eznohtbdvijwsbdks', 'xadezwhbbmlqz', 'b', 'socrdjxsibkb', 'dk', 'eznohtbdvijws', 'pavsosnncajr', 'jixlmxxmxnnbpebjhitvtsaiwzmtqq', 'yuxmmnrqz', 'mpzytweuycabvu', 'tbdvi', 'ip']) from system.numbers limit 10;

select 0 = multiSearchFirstIndexUTF8(materialize('црвтгмсрооацволепкщкпнгшкамщ'), ['гйцбсханрейщжнфбхтщбйала', 'дирдфнжпнччхаоцшрийнйнечллтгцбфедгсш', 'жфйндбффаилбндмлточиирасдзйлжбдзег', 'жвоуйфсйойфцвгзшцитсчпкч', 'ршонтбгщжооилчхрзшгсдцпзчесххцп', 'пйучихссгнхщлутвменлмм', 'хишгешегдефесо', 'знупгж', 'щчфу', 'знвтжифбнщсибеноожжметачаохфхсжосдзйуп', 'ггтоцйпгхчсбохлрчлваисивжбшбохдурввагш', 'щлийбчштбсч']) from system.numbers limit 10;
select 5 = multiSearchFirstIndexUTF8(materialize('опднхссгртрхтотлпагхжипхпитраб'), ['шфршсцешушклудефцугщцмйщлошечедзг', 'нйумйхфщцгщклдожхвосочжжислцрц', 'згтпвзцбхйптцбагсвцгтнф', 'пшичси', 'ссгртрхтотлпа', 'апзазогвсбежзрйгщоитмдкн', 'непгайтзкгштглхифмзданоихц', 'пднхссгртрхтотлпагхжипхпитр', 'ждднфлрзалшптсбтущвошрйтхкцнегшхрсв', 'брп', 'сгртрхтотлпагхжипх', 'нхссгртрхтотлпагхжипхп', 'пагхж', 'мфкжм']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('овччцнтчайомсйййоуйуучщххиффсб'), ['жжрддцпнехйр', 'шзбвуооинпаххесйкпкошжмцзгхе', 'ррсннилщлщжгцтйрпхабкехахззнтщемагдйшпсч', 'пуфугнказепщ', 'гддхтплвд', 'сщсчи', 'бйрсахедщфкхиевкетнс', 'йфжцжшпхлййхачзхнфоц', 'цтмтжлщдщофисзрвтбо', 'кщсевбоуйб', 'щгаапзкн', 'осймщовшчозцййизм', 'фкмаат', 'бкзцсдонфгттнфтаглпрцтбхбсок', 'жлмичлйнйсжбгсейбсиезщдмутационжгмзп', 'нбищижнлпмтморлхцхвеибщщлкйкндлтпбд']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('фдситчщдвхмфйтхшдтуцтщжрочщо'), ['ейшфдннтйечгк', 'фуйщгбйшдцирзб', 'ехйцмчщрсртнк', 'увтцмдорщжфгцгзущпувтщкнрфсйбщрзй', 'хчщпхвуарнббпзсцшчщуносйгщпсбтх', 'жтдчрхфмхцххккзппзбнуббс', 'тчохнмбаваошернеймгготлузвсбрщезднеил', 'стссчкшрчррйбхдуефвеепщшзмербгц', 'жбезжпещ', 'вйтсрхптлкшвавдаакгохжцоощд', 'искеубочвчмдхе', 'щмлочпзбунщнхлрдлщтбеощчшчхцелшоп', 'екуийтсйукцн', 'дочахгжошвшйжцпчзвжйкис', 'лтеенешпсболгчиожпжобка', 'букзппщрчбпшвпопвйцач']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('гопвмрутфпфбхмидшлуб'), ['цнхшдойгщн', 'дкаежщрапщпщеа', 'фмогимдничрфтхмсцмчпдфтиофнтйц', 'фчмсщисхщуп', 'ощмвдчефозйжбеесбмещочевцчд', 'апкбцйщжщабвппофм', 'мтйоддлфцгдуммптднпщшрн', 'икхнсмжчбхнфхнссгл', 'ущмунинлбпрман', 'ллкнечрезп', 'ажтнвбиччджсзтйешйффдгдрувер', 'йрщ', 'чигдкйшфщжужзлвщулквдфщхубги', 'иккшсмаеодейнкмгхбдлоижххдан']) from system.numbers limit 10;
select 12 = multiSearchFirstIndexUTF8(materialize('срлцчуийдлрзтейоцгиз'), ['жщлнвбубжпф', 'оклвцедмиср', 'нлзхмчдзрззегщ', 'хоу', 'шайиуд', 'ерслщтзцфзвмйтжвфеблщдурстмйжо', 'жмгуйузнчгтт', 'стеглмрдмирйрумилвшнзззр', 'втедлчрчайвщнллнцдмурутш', 'цимхргмрвмщиогврнпиччубцйе', 'ктчтцбснзцйцймридвш', 'ейоц']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('лрицжленфилзсжпжйнцжжупупдфз'), ['чпбрмлрнцмвеуфу', 'рмпизмпжчшбхдудчшохтжш', 'гргцжчпгщищннусв', 'ийщтщвзчшпдзитщубакусхавслрсбткб', 'бйбакижцтибгбгхжцвйчжжщжсжкзф', 'чгрп', 'чуносжусжфчмфжхрщзлщрдвбашажаанча', 'чекршбш', 'лбцкхйсооцц', 'сгвнлегвфмпчтййлрмд', 'наатущркхйимхщщг', 'щпзоеимфощулбзхафпц', 'дцабцхлврк', 'умидмчуегтхпу', 'дщнаойрмчсуффиббдйопдииуефосжхнлржрйлз', 'щзжетезвндхптпфлк', 'бгчемкццдбжп', 'иихуеоцедгрсеужрииомкбззцнгфифоаневц']) from system.numbers limit 10;
select 3 = multiSearchFirstIndexUTF8(materialize('бхжвчашрощбмсбущлхевозожзуцгбе'), ['амидхмуеийхрнчйейтущлуегрртщрхвг', 'фнисцщггбщйа', 'хжвчашрощбмсбу', 'фщвщцнеспдддцчччекчвеещ', 'ущуджсшежчелмкдмщхашв', 'цкуфбиз', 'евозожз', 'ппт', 'лвцнелшхцш', 'ощбмсбущлхев', 'ефхсзишшвтмцжнвклцуо', 'цржсржмчвмфмнеещхмиркчмцойвйц', 'ашрощбмсбущлхевозожзу', 'гхщншфрщзтнтжкмлщанв', '', 'хевозо', 'ощбмсбущлхевозожзуц', 'возожзуц']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('мзчатйжщгтзлвефчшмлшт'), ['гхшфрунирйдзтеафщгк', 'ймхмфлц', 'звуумивмвштчтнтеобзщесакийгк', 'чщжетзнцишхрммтбцакиббчп', 'блмидикавущщдпгпчхйаатйанд', 'цмщшбклгцгмчредмущаофпткеф', 'бнетввйцзпдерхщ', 'ицйнцрввемсвтштчфрпжнатаихцклкц', 'дзлщсштофвздтмчвсефишс', 'пбзртдцвгкглцфесидлвваисщр', 'ммеилбзфнчищч', 'жш', 'лздиззтпемкх', 'байлужднфугмкшгвгулффмщзхомпав', 'рсзнббедсчзущафббзбйоелид', 'цфшйкцксйгуйо']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('жжмзмащйфжщлрффбпврзнидииейщ'), ['ржфзнлйщсздйткаоцруйцгцт', 'илинксщмгщшещееифвпданмйлж', 'кг', 'гпааймцщпмсочтеиффосицхйпруйшнццвс', 'кнзфгжйирблщлл', 'ищуушфчорзлкбцппидчннцвхщщжййнкфтлрдчм', 'тбтдчлвцилргоргжсфбоо', 'ехаех', 'нехщмдлйджждмрцпйкбрнщсифхфщ', 'тцжпснйофцжфивзфбхзузщтмдкцжплавозмше']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('биаризлрвххжкпщтккучфизуршткпн'), ['йбручвндбщвссаеха', 'ол', 'еузкмпогщзгзафшдшоплбфнфдккх', 'ибзихщйфбтаз', 'ибрчиейш', 'нафрпбснзрузнтмнйиомтечтшзбкпзутдилтф', 'тщтбапцчдий', 'щкнггмфцжрзщцзжвлкчбммхтхтуж', 'ваам', 'цкфиушзигбжтацнчдлжжзфшщммтнлж', 'туфовжтнкзщсщщизмрйкхкпц', 'пирзксзикфтшодожшчцг', 'жфчфцфвлйбмеглжйдазгптзщгж', 'тутириждкзчвтсоажп', 'мотзусбхту', 'слщкгхжщфщоцкцтрлгп', 'бругтбфесвсшцхнтулк', 'восур', 'ссежгнггщдтишхйнн', 'вгзосзгоукмтубахжнзгшн']) from system.numbers limit 10;
select 8 = multiSearchFirstIndexUTF8(materialize('мчслвбжвманджййсикнврцдчмш'), ['рлбмй', 'иб', 'жажлцсзхйфдцудппефвжфк', 'огггхзгтцфслхацбщ', 'дзтцкогаибевсйещпг', 'зпцтйзфмвгщшуоилл', 'етщзгцпдйчзмфнхпфцен', 'нджййсик', 'сикнврцдчмш', 'жййсикн', 'икнврцдч', 'паокаочввеулщв', '', '', 'кечзсшип', 'вбжвманджййсикнвр']) from system.numbers limit 10;
select 2 = multiSearchFirstIndexUTF8(materialize('нвррммппогдйншбшнехнвлхм'), ['нфошцншблеооту', 'лх', 'цртд', 'огдйншбшн', 'уулддйдщицчпшбоиоцшй', '', 'дрдужзжпцкслетгвп', 'й', 'мппогдйншбшнех', 'дйншб', 'лжвофчзвдд', 'рммппогдйншб', 'ехнв', 'втущсщзбчсжцмаанчлнасп']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('удехбкабиацхпгзнхжелшц'), ['фмнбтйезсфоахофофдблкжщжфмгхтзс', 'тщтамзафозхлз', 'цшжфсбл', 'йзгзилупшллвипучхавшнмщафзмнк', 'лу', 'гтебпднцчвмктщсзи', 'лпщлмцийгуеджекшд', 'пцдхфоецфрунзм', 'зис', 'хпж', 'цтцплхцжишфнплуеохн', 'впх', 'чцчдацлуецрчцжижфиквтйийкез', 'гчшмекотд', 'пйгкцчафеавзихзтххтсмкал', 'сжфхпцгдфицжслрдчлдхлсувчнрогнву']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('щщвфчгамтжашнуошбзшуйчтшх'), ['дийу', 'жеомлсжщймемрсччошдфажцтдп', 'нгопнцквбф', 'хопб', 'ив', 'чвфвшфрдфелрдбтатшвейтг', 'вхкцадмупдчбаушшлдксйв', 'жтжбсвмшшсйеуфдпбдлкквдиовж', 'гтсдолснхесйцкйкмищгсзедх', 'ошплп', 'ифпуррикбопйгиччи', 'чдфймудаибвфчжтзглс', 'зпцмвпнлтунвйж', 'еждрйитхччещлцч', 'вмофсужхгрнзехкх', 'щжгквкрфжмжжсефпахст']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('рфгигуужжцфмоаешщечувщгонт'), ['слащченщлуоцргврбаб', 'тцизут', 'лйрсцолзклжбчрзгббммоищщ', 'уицмлоилзф', 'зпхмшвфйккфщщп', 'ймижрпдщмшв', 'пуощжлрмжлщхмкйгщшщивдпчпжчл', 'ойахшафнж', 'гксомбвцрсбжепхкхжхнсббци', 'панлраптщмцмйфебцщемйахенг', 'сохлгожштлднчсзпгтифсйгфмфп', 'аждчвзну', 'дхшуфд', 'борзизцхнийбщгхепрнзшй', 'фщшздруггрке', 'оевупрйщктнолшбкунзжху']) from system.numbers limit 10;
select 8 = multiSearchFirstIndexUTF8(materialize('кщзпапйднучлктхжслмищ'), ['апмдйлсафхугшдезксш', 'кйрм', 'цйивайчшуалгащсхйш', 'злорнмхекг', 'сгщврурфопжнлхкбилдч', 'бнлпщшнвубддрлижпайм', 'нукдонццнрмовфнбгзщсшщшдичежффе', 'йднучлктхжс', 'зпапйднучлктхж', 'затйотдсмпбевлжаиутсуг']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('жцажссефррршнфмнупщаоафгкщваа'), ['жфпщкгзкрмтщчцтжйчпйдошбшоцд', 'бхгйлйдробптвущшппзуиидежнлтпбжащткцф', 'хлещазйцепдханпажчизнхгншйуазщхй', 'ашцк', 'фрбммхдднчзшс', 'нжцанилзжаречвучозрущцдщаон', 'длмчзцрмжщбневрхуонпйейм', 'шкбщттврлпреабпоиожнууупшмкере', 'вуцпщдиифпеоурчвибойбпкпбкйбшхдбхнаббж', 'нртжвкдйтнлншцанцпугтогщгчигзтоищпм', 'цкплнкщлкшемощмстздхпацефогтск', 'цвждйбсмпгацфн', 'шсжшрзрардтпщлгчфздумупд', 'цйииткглчжйвуейеиииинврщу', 'унлодтулшпймашоквббчйнибтвалалрвбцж', 'нбнфнвйишйжлзхкахчмнлшзуеенк', 'бшлпсщжквпцахигчдтибкййб', 'фчакпзовтрлкншзцулшщмпзж']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexUTF8(materialize('иматеччдфлггшпучумджпфпзмвх'), ['дахахпчлцлаачгцгтфпнжлшчйуцбшсг', 'атжйувхец', 'грдсбвиднницдвшпйршгмегцаоопнжгй', 'чзлхречмктфащмтеечуиагоуб', 'савбхлпилийщтихутйчдгфсойй', 'вбгочбзистзщшденусцофит', 'мар', 'дфшажхдсри', 'тжлмщшж', 'птсрсщгшммв', 'ре', 'зратамкткфкинййй', 'гуцмсизулвазужфдмхнелфнжббдтрудчтнфцр', 'нйчинеучкхнпчгнйвчвсвлгминуцахгщввжц', 'ечагчнуулфббгбел', 'йшжуговрйкащцофдокфчушжктнптйеззушфо']) from system.numbers limit 10;
select 11 = multiSearchFirstIndexUTF8(materialize('азтммйтшхцхлгдрнтхфжбдрлцхщ'), ['нпучщфвспндщшспзмшочгсщжчйгжбжзжжтн', 'хккдйшабисдузфртнллщпбоуооврайцз', 'йпхрфжждгпнйаспйппвхбргшйвжччт', 'ффеее', 'кежцновв', 'еххрчштарзмкпйззсйлмплхбчбулзибвчбщ', 'шфжйдотрщттфхобббг', 'ожоцжущопгоцимсфчйщцддзнфи', 'цуимеимймкфччц', 'прммщмтбт', 'хцхлгдрнтхфж', 'лгд', 'цжбдаичхпщзцасбиршшикджцунйохдлхй', 'пидхцмхйнспйокнттмййвчщпхап', 'йтйзмеаизкшйошзвфучйирг', 'хцхлгдр']) from system.numbers limit 10;

select 0 = multiSearchFirstIndexCaseInsensitive(materialize('gyhTlBTDPlwbsznFtODVUzGJtq'), ['seSqNDSccPGLUJjb', 'xHvtZaHNEwtPVTRHuTPZDFERaTsDoSdX', 'QCeZOYqoYDU', 'bsybOMriWGxpwvJhbPfYR', 'FFHhlxfSLzMYwLPPz', 'tvDAJjaLNCCsLPbN', 'kOykGaSibakfHcr', 'mWAZaefkrIuYafkCDegF', 'ILrFDapnEDGCZWEQxSDHjWnjJmeMJlcMXh', 'zHvaaTgspUDUx', 'tss', 'laUe', 'euUKFLSUqGCjgj', 'Kd', 'MxyBG', 'qRXMsQbNsmFKbYSfEKieYGOxfVvSOuQZw', 'PdBrNIsprvTHfTuLgObTt', 'kMekbxI']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('ZxTznPEbfoBfLElYOrRiHrDLMmTpIh'), ['bJhYwKLeeLvLmXwWvQHWFkDQp', 'dLyZmUicTZmUfjfsFjxxgOiMJn', 'UCYbbGcY', 'kpPiwfWHEuh', 'jviwmHeiTQGxlTKGVEnse', 'cVnEyLFjKXiLebXjjVxvVeNzPPhizhAWnfCFr', 'gkcoAlFFA', 'ahZFvTJLErKpnnqesNYueUzI', 'VIJXPlFhp', 'rxWeMpmRFMZYwHnUP', 'iFwXBONeEUkQTxczRgm', 'ZnbOGKnoWh', 'SokGzZpkdaMe', 'EfKstISJNTmwrJAsxJoAqAzmZgGCzVRoC', 'HTmHWsY', 'CpRDbhLIroWakVkTQujcAJgrHHxc']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('VELfidaBvVtAghxjkrdZnG'), ['fvEFyRHvixuAYbuXygKeD', 'zFNHINreSOFksEGssBI', 'hcdWEcKDGWvfu', 'KczaFjvN', 'nZLTZAYSbfqcNWzWuGatDPUBYaRzuMBO', 'UdOdfdyPWPlUVeBzLRPMnqKLSuHvHgKX', 'DgVLuvxPhqRdSHVRSeoJwWeJQKQnKqFM', 'NNfgQylawNsoRJNpmFJVjAtoYy', 'tWFyALHEAyladtnPaTsmFJQfafkFjL', 'lYIXNiApypgtQuziDNKYfjwAqT', 'QjbTezRorweORubheFFrj', 'htIjVIFzLlMJDsPnBPF', 'ltDTemMQEgITf', 'fprmapUHaSQNLkRLWAfhOZNy', 'dOJMvPoNCUjEk', 'm', 'vEEXwfF', 'aVIsuUeKGAcmBcxOHubKuk']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('kOzLaInSCOFHikwfkXaBfkyjdQ'), ['t', 'emHGfAiZSkZaVTSfplxRiPoDZUTT', 'YHnGJDTzxsboDsLPGHChMHwrHHICBIs', 'gbcbVHSlVeVDOeILWtSLkKfVVjG', 'fPaJjbnNthEwWZyg', 'qS', 'PCQxoLaSdQOjioMKPglmoWR', 'KLMNszm', 'TCErEFyxOvqnHs', 'dRbGzEJqvIGAcilZoHlXtZpjmLLZfsYueKqo', 'iKHmNSbGgaJYJEdMkbobXTdlFgAGEJMQ', 'mUGB']) from system.numbers limit 10;
select 1 = multiSearchFirstIndexCaseInsensitive(materialize('JGcICnWOGwFmJzHjtGJM'), ['fmJzHj', 'LhGTreYju', 'yCELHyNLiAJENFOLKOeuvEPxDPUQj', 'kWqx', 'OBnNMuaeQWmZqjWvQI', 'ektduDXTNNeelv', 'J', 'iCNwoGwfMJzhjtGJ', 'uiIipgCRWeKm', 'bNIWEfWyZlLd']) from system.numbers limit 10;
select 7 = multiSearchFirstIndexCaseInsensitive(materialize('fsoSePRpplvNyBVQYjRFHHIh'), ['ZqGBzyQJYuhTupkOLLqgXdtIkhZx', 'pouH', 'mzCauXdgBdEpuzzFkfJ', 'uOrjMmsHkPpGAhjJwVOFw', 'KbKrrCJrTtiuu', 'jxbLtHIrwYXDERFHfMzVJxgUAofwUrB', 'PLvNyBVQYjRfhhi', 'wTPkeRGqqYiIxwExFu', 'PplvNybvqyJ', 'qOWuzwzvWrvzamVTPUZPMmZkIESq', 'ZDGM', 'nLyiGwqGIcr', 'GdaWtNcVvIYClQBiomWUrBNNKWV', 'QQxsPMoliytEtQ', 'TVarlkYnCsDWm', 'BvqYJr', 'YJr', 'sePrPPLVNYbvqYJRFhh', 'ybvq', 'VQYjrFHh']) from system.numbers limit 10;
select 3 = multiSearchFirstIndexCaseInsensitive(materialize('aliAsDgMSDPISdriLduBFnuWaaRej'), ['gWOFTxMrQGQaLrpJamvRhgeHwk', 'iWsBLzLycWvbJXBNlBazmJqxNlaPX', 'Ri', 'FPLRURSsjvsySncekcxaWQFGKn', 'wgXSTVzddtSGJQWxucYorRjnQQlJcd', 'wOLJWZcjHEatZWYfIwGIqnuzdcHKSFqfARfNLky', 'eEECZMNmWcoEnVeSrDNJxcOKDz', 'duBF', 'EhfLOjeEOQ', 'dUbFNUWA']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('EUzxPFYxMsJaTDzAKRXgZIVSFXU'), ['TDKAgICICjzBKHRqgFAuPCSODemldGGd', 'LvMluSJTIlgL', 'srbRhQKjPIchsipVHsjxwhK', 'vdurVsYkUWiFQVaDOnoNIJEX', 'UzZsZqAUNjMvWJaTqSWMHpzlDhVOaLzHPZfV', 'XcnnPXXEJJv', 'JSwFBNnYzNbIRZdeMfYiAfxzWfnCQFqoTUjns', 'HBMeqdLkrhebQeYfPzfJKAZgtuWHl', 'cMfSOnWgJvGhFPjgZdMBncnqdX', 'orDafpQXkrADEikyLVTHYmbVxtD', 'Vz', 'bfYwQkUC', 'q', 'YqomKpmYpHGv']) from system.numbers limit 10;
select 4 = multiSearchFirstIndexCaseInsensitive(materialize('mDFzyOuNsuOCSzyjWXxePRRIAHi'), ['TfejIlXcxqqoVmNHsOocEogH', 'clyblaTFmyY', 'JQfxMAWVnQDucIQ', 'jw', 'fGetlRA', 'uWwCOCd', 'rInhyxSIFiogdCCdTPqJNrqVaKIPWvLFI', 'mimSJjfCWI', 'jqnJvNZXMEPorpIxpWkhCoiGzlcfqRGyWxQL', 'bxCJeVlWhqGHoakarZcK', 'unsUOcSZyjwxxe', 'E', 'PR', 'nsUoCSZyjwxXEPr', 'sfotzRPMmalUSjHkZDDOzjens', 'zYJwxx', 'DFzyouNsUocsZ', 'QBaQfeznthSEMIPFwuvtolRzrXjjhpUY', 'sQPVBaoeYlUyZRHtapfGM', 'lPiZLi']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('VOAJfSkbDvNWAZNLIwqUgvBOddX'), ['pHrGGgJ', 'VohjtPdQZSNeKAlChDCnRTelroghFbZXVpnD', 'rnWebvdsmiFypMKL', 'NtKRiJOfAkWyKvubXrkOODgmZxvfOohsnHJEO', 'nxsDisKarasSZwESIInCJnYREUcoRUTXHBUH', 'mXYYr', 'jujScxeTBWujKhKyAswXPRszFcOKMSbk', 'INEegRWNgEoxqwNaGZV', 'VVyjMXVWVyuaOwiVnEsYN', 'mkLXSmXppxJhFsmH', 'pRVnBrWjqPeUDHvhVuDbzUgy', 'PzchFdPTkOCIVhCKml', 'KXaGWnzqoHBd', 'PhzQVqIOLleqDSYNHLjAceHLKYPhCVq', 'aixxTqAtOAOylYGSYwtMkZbrKGnQLVxnq', 'ruEiaxeRaOOXGggRSPlUOGWSjxh', 'prSULtHvDMw', 'vEpaIIDbGvIePYIHHZVNSPYJl']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('ZHcEinZEFtfmHBLuCHntUhbIgY'), ['GKElMPEtmkLl', 'mkrzzjSRfXThuCQHkbZxRbhcymzTxcn', 'PREwQjxBJkpkiyuYEvtMZNFELgbINWsgf', 'lFEGlPtaDJSyoXzwREiRfpzNpsaBYo', 'tmVTuLPhqhgnFNhHvqpmc', 'NtijVhVfAwpRsvkUTkhwxcHJ', 'O', 'FSweqlUXdDcrlT', 'uljEFtKVjIzAEUBUeKZXzCWmG', 'dBIsjfm', 'CNaZCAQdKGiRUDOGMtUvFigloLEUr', 'yWjizKZ', 'QqPVdyIFXcweHz', 'uPmgGWGjhzt']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('AYMpbVsUQqAfoaMiJcYsulujYoSIx'), ['aXECumHNmAEefHPJy', 'hTosrERBdVCIilCYcMdHwaRh', 'PVDBpwrc', 'uFvQRPePvmzmocOauvEjqoxMhytzOwPSOCjmtm', 'kQqIlSCHDmWXCKN', 'ybAHGYDEDvvOJsF', 'WpkANi', 'cFGuzEcdahZtTdLFNBrRW', 'EBaybUFxO', 'mRlZUzHzMsMAgvtRtATEDLQvXZnZHw', 'uqxckjqpCBHiLgSPRz', 'Lv', 'AJcRfAvBmQVMOjaFfMfNHJt', 'FYsPM', 'pkKXTPgijOHFclqgVq', 'Ck']) from system.numbers limit 10;
select 11 = multiSearchFirstIndexCaseInsensitive(materialize('gmKSXWkNhKckrVNgvwiP'), ['bdJMecfCwQlrsgxkqA', 'NTgcYkMNDnTiQj', 'fmRZvPRkvNFnamMxyseerPoNBa', 'rfcRLxKJIVkLaRiUSTqnKYUrH', 'YSUWAyEvbUHc', 'PridoKqGiaCKp', 'quwOidiRRFT', 'yHmxxUyeVwXKnuAofwYD', 'gichY', 'QlNKUQpsQPxAg', 'knhkCKRVNGvWIp', 'jAuJorWkuxaGcEvpkXpqetHnWToeEp', 'KnHKCKrvNgVW', 'tCvFhhhzqegmltWKea', 'luZUmrtKmmgasVXS', 'mageZacuFgxBOkBfHsfJVBeAFx', 'hKC', 'hkRCMCgJScJusY', 'MKSXWknHkckrVNgv', 'osbRPcYXDxgYjSodlMgV']) from system.numbers limit 10;
select 15 = multiSearchFirstIndexCaseInsensitive(materialize('lcXsRFUrGxroGIcpdeSJGiSseJldX'), ['pBYVjxNcQiyAFfzBvHYHhheAHZpeLcieaTu', 'SQSQp', 'OQePajOcTpkOhSKmoIKCAcUDRGsQFln', 'AYMDhpMbxWpBXytgWYXjq', 'gkUC', 'oWcNKfmSTwoWNxrfXjyMpst', 'fQSqkjRNiBGSfceVgJsxgZLSnUu', 'LRrhUjQstxBlmPWLGFMwbLCaBEkWdNJ', 'cZnaActZVoCZhffIMlkMbvbT', 'Uxg', 'vlKdriGMajSlGdmrwoAEBrdI', 'Fl', 'XzcNdlUJShjddbUQiRtR', 'AqowAuWqVQMppR', 'SRFUrGXrOgiCP', 'k']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitive(materialize('KhwhbOzWvobUwJcteCHguFCn'), ['LkDYrpvDfPL', 'CIaTaShobVIaWjdbsNsCMdZKlGdtWuJmn', 'zYcsxxFyfuGrPdTPgEvGbXoYy', 'vDIeYpJbLMGMuRkIrPkAnqDDkqXPzy', 'Ievib', 'CREiuEsErFgvGEkQzThHtYtPmcL', 'JjRWKyALtSkoGmRxh', 'JxPhpijkDOpncCKyDEyXvKNua', 'jo', 'mKpFscuBEABMAlQO', 'qiFTgJpcnUMRKzTEuKY', 'pXBtITxCPRaXijM', 'guYVLpIbu', 'tSKYIxv', 'oDnWaFAmsXGRdGvRPhbCIvFSFQNlSVYB', 'phdckINUiYL']) from system.numbers limit 10;
select 14 = multiSearchFirstIndexCaseInsensitive(materialize('pXFoUGwVTAItBqgbBaQwAqmeh'), ['LfBevBpGnaSlmGhbeZ', 'NtBYzEksiXvYI', 'jMeRw', 'omtaduY', 'BsWyvNdkfXsTBxf', 'CtoOIvaesuca', 'pgJcRIBVbyaPBgGsNKP', 'bAwdUMnwKvMXfFHQWrtfMeqcORIJH', 'GDxZblrqWSxUJFjEuXArPtfHPdwSNGGL', 'LLxcfp', 'NrLghkFpwCdvHJBfPBgiMatNRaDKjO', 'XCzr', 'cCojPpfLkGZnaWBGpaZvrGMwgHNF', 'BaQWAQmE', 'AQ', 'RtxxEZDfcEZAgURg']) from system.numbers limit 10;
select 5 = multiSearchFirstIndexCaseInsensitive(materialize('KoLaGGWMRbPbKNChdKPGuNCDKZtWRX'), ['FBmf', 'QJxevrlVWhTDAJetlGoEBZWYz', 'tKoWKKXBOATZukMuBEaYYBPHuyncskOZYD', 'kgjgTpaHXji', '', 'xOJWVRvQoAYNVSN', 'YApQjWJCFuusXpTLfmLPinKNEuqfYAz', 'GXGfZJxhHcChCaoLwNNocnCjtIuw', 'ZLBHIwyivzQDbGsmVNBFDpVaWkIDRqsl', 'Kp', 'EyrNtIFdsoUWqLcVOpuqJBdMQ', 'AggwmRBpbknCHdKPgun', 'xNlnPtyQsdqH', 'hDk']) from system.numbers limit 10;
select 6 = multiSearchFirstIndexCaseInsensitive(materialize('OlyNppgrtlubvhpJfxeWsRHpr'), ['slbiGvzIFnqPgKZbzuh', 'fakuDHZWkYbXycUwNWC', 'HnVViUypZxAsLJocdwFFPgTDIkI', 'bLx', 'fmXVYOINsdIMmTJAQYWbBAuX', 'pjFXews', 'BG', 'vrSQLb', 'ub', 'pREPyIjRhXGKZovTqlDyYIuoYHewBH', 'hnNQpJmOKnGMlVbkSOyJxoQMdbGhTAsQU', 'UwaNyOQuYpkE', 'yHNlFVnuOLUxqHyzAtNgNohLT', 'YJRazuUZkP', 'z', 'lUbVhpjFxEWsRhP']) from system.numbers limit 10;
select 6 = multiSearchFirstIndexCaseInsensitive(materialize('ryHzepjmzFdLkCcYqoFCgnJh'), ['cLwBRJmuspkoOgKwtLXLbKFsj', 'YSgEdzTdYTZAEtaoJpjyfwymbERCVvveR', 'RzdDRzKjPXQzberVJRry', 'HUitVdjGjxYwIaLozmnKcCpFOjotfpAy', 'LWqtEkIiSvufymDiYjwt', 'FDlKCCYqoFCGNj', 'jmZfdlKCcyQOFcGnJ', 'OZCPsxgxYHdhqlnPnfRVGOJRL', 'JfhoyhbUhmDrKtYjZDCDFDcdNs', 'KCCYqo', 'EPJMzFDLKcCYQ', 'zLQb', 'qsqFDGqVnDX', 'MzfdLkCCyQOFc']) from system.numbers limit 10;
select 5 = multiSearchFirstIndexCaseInsensitive(materialize('oQLuuhKsqjdTaZmMiThIJrtwSrFv'), ['MsfVCGMIlgwomkNhkKn', 'fBzcso', 'meOeEdkEbFjgyAaeQeuqZXFFXqIxBkLbYiPk', 'tNV', 'i', 'EwuTkQnYCWktMAIdZEeJkgl', '', 'hUo', 'dtAzmMITHijRtwsrFV', 'vhnipYCl', 'puor', 'TazMmiTh', 'ITHIJRTWSrf', 'luuHksqJDTaz', 'uHkSQjDtazMMiThIjrtwSRFV', 'gpWugfu', 'QjdtazmmIthIjRTWSRFV', 'ZdJpc']) from system.numbers limit 10;

select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('ИпрУщйжббКВНИчйацпцоЛП'), ['цШСкЕвеГЕЗЦщруИБтЦсБГАу', 'Хнщта', 'БшА', 'СалШйР', 'ЩфДГРРчшБДММГЧоноЖСчдпВХшшгйН', 'бЕжПШЦддожнЧоЕишчшЕЙфСщиВПФМ', 'ТЗзГФх', 'Чфл', 'КнНкнЖЕкППварНрхдгЙкДешмСКИЛкеО', 'ЖИсЧПСФФМДиТШХЦфмЗУпфрУщСЛщсфмвШ', 'ллЙумпхчОсЦМщУ', 'ГМУНЦФшНУбРжоПвШШщлВФАтоРфИ', 'БХцжеНЗкжЗЗшЦзфгдЖОзЗЖщКМИШАтЦАп', 'мтСкЕнбХШнЛхХГР']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('цмйвГЖруДлдЦавхЖАлоЕхЗКд'), ['ХфБПМДВХЙ', 'саЗваАбднХбЦттмКсМбШбВМУйНКСЖжХЦНц', 'плиЩщШАцЖсхГ', 'ЗнУЕФЗВаНА', 'ЧДйСаЗГЕшойСжбсуЩуЩщбПР', 'ЧЕуЩкФБВвчмабШЦтЖбОрЗп', 'йХбМсрТАФм', 'РЖСЗвЦлНВПЧщГУцЖ', 'ГГлЩрОХКнШРТуДФ', 'шСабРжла', 'ЕчБвгаРЧифаЙщХПпГЦхчШ', 'дайшйцВНЩЧуцйдМХг', 'УнзНКЧххВрцЩМлАнЖСДОДцбИгЛЛР', 'сЛЗзПбиАгзК']) from system.numbers limit 10;
select 2 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('дфЧлзОжММФкЖгиЗЩлоШжФТкцк'), ['ЗРТцИрсФСбПрщГЗ', '', 'ЖГИЗщлОш', 'АДПН', '', 'чЛЗОЖмМфКжг', 'Мфкж', 'ндаовк', 'зГЛРГАНШмСмШМефазшеБкзДвЕШиЖСЗЧПИфо', 'ФЧЛзОЖммфКжгиЗЩ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('ИИКДМЛхРчнвЙЕкВЧелТйЛВТ'), ['АчшОЛтНЙуЦЛЙфАКУйуТЗМеЗщОХТМЗеТА', 'НЦУДбчфРТОпЛкОгВпоО', 'неДавнНРеАУфТтфАнДчтнУМЛПШнроАчжш', 'бГржВПЧлЛтСВТтаМЦШШ', 'БщГщРнБхЕЛоЛсмЙцВЕГ', 'цбАжЦРеу', 'ХсЦРаНиН', 'нббДдВЗРС', 'змОПпеЛЖзушлнДЛфчЗлцЙЛфЖрЛКг', 'фШиЖСУоаНПйИВшшаоуЙУА', 'ЛктХиШРП', 'МапщВйцХч', 'жмУТкуГбУ', 'сйпзДЩоНдШЕТбПзФтсрмАФГСз', 'ЛБУвйладЕижрКзШУАгНЩчЕмАа', 'мЧпФлМчРбШРблмтмПМоС']) from system.numbers limit 10;
select 8 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('ПоДУЗАтХншЦатИшХвмИЖчГнжчНцух'), ['жЛЧХзкжлиЛцЩбЧСнЛУжЖпКРвиСРН', 'шадмЩеУШБврУдЕБЗИгмЗЕФШчЦБСзПидтАлб', 'йпГмШСз', 'хЖФЙиПГЗЩавиЗЩйПнБЗЦЩмАЧ', 'ХесщтлбСИуЦ', 'вар', 'ЙкМаСхаЩаЗнФЩфКжПщб', 'ОдУзАТХншЦатИШхвМиЖчгнЖч', 'ЗВЗДБпФфцвжУКвНсбухссбЙКЙйккЛиим', 'гХхсГЛшдфЖЛбгчоЕмоЧр']) from system.numbers limit 10;
select 7 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('ихзКЖЩсЧРСЖсЖжЛАшкТхИйТгМБпск'), ['ДРОБм', 'нз', 'тОЛ', 'щРзуЖрТ', 'Мдд', 'АЦГРК', 'Чрсжсжжл', 'чРсжсЖжл', 'ктхИйтГмБ', 'аАзЙддМДЦЩФкРТЧзЧПУойоТхБиЧПлХДв', 'иЙтгМбп', 'РицлПн', 'йДГнЧкЕв', 'ВМЩцАш', 'хКЩнДшуБЕЛТФГВгцБПРихШЙХгГД', 'иЙТГМ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('жггкщцзщшамдбРЗжйТзвхшАпХСбе'), ['лВТвтлРБжиЛЦвРЦкАЦаНБгуОН', 'рШаавцжзМрзВЧДРСузб', 'оемрЗМгФБНмжп', 'ЛбмХбФЧШГЛХИуТСрфхп', 'ЖшТдтЧйчМР', 'ЧнИМбфУпмЙлШЗТрТИкКИЩОЧеМщПЩлдБ', 'ГвРдПжГдБаснилз', 'уТнТчТРЗИЛ', 'ИТЕВ', 'дИСЖпПнПСНОвсЩЩшНтХЧшВ', 'штабтлМнсчРЗтфсТЩублЕЧйцеЦТтХ', 'ХбхгУШвАзкшЖ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('нсЩЙЕМмЧЛСйФцГВМиатГХш'), ['КсОПЧИкВсКшРхнкхБжду', 'мШмпТащжФ', 'ББЖнианЧЦпмрГЩГМаЛКжА', 'арИжзжфГТУДИРРРбцил', 'дфдмшМИщТиЗПруКфОнСЦ', 'Рцч', 'гмДгВДАтсщКЗлхвжЦУеФДАТГЙЦЧОЗвРш', 'чфХЩсДбУбВжАМшРлКРщв', 'нцБйсУ', 'фасДЕчвчДмбтЖХвоД', 'аБЧшЖшЖАКргОИшпШЧзТбтфйвкЕц', 'ЗжжсмкжЛд', 'щщлПзг', 'бП']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('сКиурчоиаЦйхгаУДПфчИтИК'), ['МЧПцУАМрХКЧмАЦннУшмРчкЖКХвху', 'РвДуВиашрРКкмжшЖНШБфлцжБЦР', 'йМУиУчНЧчРшДйБЗфЩЦйПсцгкДС', 'НсмаЛзЧвНЦШФуВРпзБГзйКцп', 'ЖлМЛУХОБллСЗСКвМКМдГчЩ', 'ЩХПШиобЛх', 'аФАЖВтРиЦнжбкСожУЖЙипм', 'аУГжУНуМУВФлж', 'ШБчтЗкЖНЙк', 'ЩоГПГчНП', 'мВЗйЛаХПоЕМХиИйДлшРгзугЙЖлнМппКЦ', 'вчмДФхНеЦйЗсЗйкфпОщПтШпспИМдГйВМх', 'ИЗИжЧжаГЩСуцСЩдкскздмЖЦ', 'дАмфЕбгс', 'ГМттнхчЩжМЧДфщШБкфчтЧ', 'ШЕииФБпщЙИДцРиЖжЩл', 'ОпуОлБ', 'хБ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('риШМбгиПЖннНоЧргзГзеДпЛиНт'), ['икДкбйдройВУсвФзрПСусДнАШо', 'чуУеТкУВФхз', 'ЕГпйчехЗвЛлБблЧПДм', 'зеоЩЧожКЛбШЩдАрКБНйшКВШаЗгПш', 'виФКуЗОтгВмТкБ', 'цДрЙгЗРаЧКаМДдБЕЧзСРщВФзПВЧГвЩрАУшс', 'мБЗИУдчХХжТж', 'ФТНМмгЖилуЛйМ', 'ЗегЩЦнЦщцИк', 'оГОусхФсДЖДЩИЕХЗпсПЩХБТГЕп', 'АУКНзАДНкусВЧХвАж', 'КвКрбсВлНАоЗсфХОйЦхТ', 'вФдеХацЧБкрхМЖЗЧчКшпфВчс', 'йХшиОвХЗжТпДТбвУрпшЕ']) from system.numbers limit 10;
select 11 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('МойрЙлтЖйБдББЛЕЕЦузЛфпИЕГт'), ['ПОжЦЩа', 'СШзЧФтСЗохЦЗдФтцНТу', 'вЕдТ', 'ечУФаМДнХщЕНУи', 'вмеосТзБАБуроЙУЛгФжДсЧщтчЕзлепгк', 'ИЧтБрцПмРаВрйИвНЛСйпЖжУВдНрурКшоКХП', 'ЕН', 'щКЦЩгФБСХпкпит', 'ей', 'ЕахшеОМРдЕГХуГЖчвКХМЕ', 'Гт', 'НужЛЛЙОАл']) from system.numbers limit 10;
select 11 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('еззЦАвУаДнзИКЙнЙдртРоП'), ['КгЩбшПЛКвтИРцйчккгЧчЧмтГ', 'кЛппСФщзМмТйВЕтбЩЦлО', 'ШпдзиЖх', 'иИХ', 'пУаАФгсмтофНФХиЦЕтТЗсОШЗЙ', 'фаКАБТцФМиКЖрИКшГБЗБ', 'идЖЙдЦММУнХЦЦфсФМ', 'МиЦечЖЦЙмРВЙОХсБРНнрлйЙшц', 'ТфдСтМгтмимТМАучтхПНЦлуф', 'бейККЛСггУЦБсокЕЙпнРЧ', 'цавУАДНЗИКЙнЙд', 'ЩйЕЖчЧщаПшжФсхХЛЕТчвмЙнуце', 'РТРОП', 'цАВуАДнзИкЙНЙдРтРо', 'аЩПИд', 'ОСчКшОАчВмр', '', 'уЙЛИуЕУвцДшНОгбТбИШв', 'АВУаднзИКЙНйдР', 'жТйоП']) from system.numbers limit 10;
select 12 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('шйМЦУщвфщшбмлТНВохСЖНУ'), ['хшТАпТоШхКНсДпвДЕчДМНбАНччд', 'ХКуПСтфСйРжмБглОШЙлйДкСФВйВ', 'хпмНЦМУШеАД', 'чзмЧВвлбЧкАщПкзТгеуГущб', 'шзжрДд', 'еЗГОЙНйИБЗДщИИНицмсЙЗгФУл', 'кнщЙхооДТООе', 'всзЙнТшжФЗДБДрщВДлбвулДИаз', 'мп', 'уБОйцзнМпИсксхефбдЕЛйгИмГШГЗЩ', 'ОМпзШШщчФФнвУЧгжчиндЧч', 'щВФЩШбмЛТн', 'бм', 'БпфнкнйЗцПдЧЩбВ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('НЗБлОбшмОПктткоччКиКрФсбкШАХ'), ['нффЕББУЖГшЖвГфЦФГЕСщсЩЧлфнАшшктизУ', 'нСмпцхшИои', 'ЧИчЗУтйЦхГезппФРХХШуцЗШВ', 'РИнщН', 'НЩдВТсЙсОдхРбМФнСпАбОПкудБФСчмб', 'йхглпдКтртгош', 'ибгУРАБцх', 'ИЕиЛрИДафмЗИкТвАуГчШугбЧмЛШщсОЧбБкП', 'ЩСМуХМ', 'АУсмдЗБвКфЩ', 'пгбТНОйц', 'МоИ', 'КОйкзОЕИЗМЩ', 'чщттЛРНнГхЗхХй', 'ЩшцЧРКмШЖЩЦемтЧУЛГкХтВНзОжУХТпН', 'ЕшбБНчрДпЩЧМлераУЖХйфйдчтсчПШ', 'дбФйтИАшДйЩтбФйШуПиРлГмВОШаСлШЧИвфЖщгж', 'ОДжТЦщпщИжфуеЩмн', 'ПЛНЕзжСчВКДттуФРУ', 'БбмеГЩХшжрцОжХНииВКВлдиХБДСмнНфХЛТХ']) from system.numbers limit 10;
select 4 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('ЕКаЖСЗЗЕЗгПдШкфцЙТцл'), ['ЙКМИХРОХ', 'НвМУХзфчДбАРЙДу', 'чмщжФшшжсЗТв', 'жСЗзеЗг', 'ЛФсКзВСдЦД', 'АЖсЗЗЕЗГ', 'Пдшкфц', 'усйсКщшрДрвнФЛедуГХ', '', 'цйтЦ', 'Ощс', 'ЕЗГпдшКф', 'ззеЗгп', 'УгЛйхШТтшрЛ', 'ЗзЕЗгП', 'КЛмТЩРтрзБбЩРгФбиОБазУнтУЦ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('чЕжАфАрБпКбДмшАшТШККауЩИхНВО'), ['ЧЙпЗЧЧлйПЙЖЙшККг', 'зйхуМЩАИПГЗА', 'ЙцехноХниИбзБЧ', 'чВомЗОфУроС', 'дбРхХЗрзоДДШщЕДжиФаЙ', 'еЛзТцЩДиДГрдМОНЧУнеТуДЩЧЦпГЕщПОРсйпЧ', 'ФчнпМРЧцПЙЩЩвфДХПнУхцЩСИ', 'цлШеУкМБнжЧлУцСуСЙуотшМфйс', 'лугГлкщКщкзЛйпбдсишргДДшОувр', 'ЗРИаФЛЗФрСзм', 'аЗвжВгхЩоЦ', 'чГКлеБНДнИЖЧеШЧДнИвсГДЖЖфБМНсУЦосВс', 'щЦнПУзЧщнЩЕ', 'рВУв']) from system.numbers limit 10;
select 20 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('анктгЦВВкЧвЖиБпфТйлр'), ['НшДПчтсСЧпкидаХжаЙчаДчЦГшГ', 'ХнцЛШИрХВаРхнЧИЙрОЛЛИТпППфгЖЩФ', 'ФАЛущПупмдМБмтйзУшрВМзцзШжгД', 'ГчЛЧеЛДХеипдшЦЦмаШНаРшУТ', 'фОЕфжО', 'ТНсУАнчшУЛЦкцчЙ', 'ЛйЦКБЗГЦйКЩиОПуТЦкБкБувснЙи', 'Бунф', 'ИтХЛШСУНЦВйРСЙчДчНвйшЗЦй', 'АцСКнзБаЖУДЖегавйБгужШАДЙтжИВк', 'ЦцХщфирДПрСуХзхЖМЕщ', 'кфдБЖКншвУФкЗДКуЙ', 'СкиСЦЗЦРмгЦНпБхфХДЙщЛзХ', 'йУепВЖАПНбАЩуЛжвЧпхМ', 'БпЧшпДочУвибщерйхйтОБАСПнЧМИОЩ', 'чФгНЗщвхавбшсООоВштбЧ', 'уДиЕцнЙХВЕйИАГдЕ', 'тп', 'ЧЕРЖсгВ', 'вЖибПФТЙЛ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('ипозйпхЛОЛТлСМХЩдМвМгШИвГиЛп'), ['ФСГзиГррБДНКГЛХбААФхИ', 'гегпАвхДЕ', 'ЦХжзщХИвхп', 'ЗЖ', 'ХОКцКзЩо', 'абИОрГПМТКшБ', 'кмХТмФихСЦсшУдхВбИШМНАНмпмХОЗйПЩч', 'еОжТСкфЕТУУжГ', 'НтщМЕПЧИКЙКйй', 'ежСикИвйЛж', 'ушЩФОтпБзЩЛЗЦЧЙиВгБЧоПХНгОуАДТЙж', 'фМЕРефнутпнцФРнрГЖ', 'хшДЧзнХпфорвЩжмГРЦуХГ', 'ЧЖн', 'вВзгОСхгНумм', 'ЗДоВлСжпфщСКсщХаолЛнЛЗбСхвЩвЩНоЩЩМ']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('МрЗтВФуЖРеЕШЧхПФбжжхчД'), ['щжОожЦндцШйТАй', 'йуРСЦУЗФУЦПвРфевСлфдРещЦтИтЩЩТг', 'ЕГЧдмХмРАлнЧ', 'йнкФизГСЗнуКбЙВЙчАТТрСхаЙШтсдгХ', 'ЧПрнРЖЙцХИщ', 'зЕ', 'СжВЩчГзБХбйТиклкдШШИееАлЧЩН', 'МШщГйБХжЙпйЕЗТзКмпе', 'НКбНщОМДзлдЧОс', 'НчзВХОпХХШМОХФумБгсрРЧИчВтгутВЩо']) from system.numbers limit 10;
select 0 = multiSearchFirstIndexCaseInsensitiveUTF8(materialize('упТУЖелФкЧЧУЦРжоБтХсжКщД'), ['щКшуОЖааЖйнЕбДИжМК', 'ЕкнШцХРВтНйШоНбЙйУоЧщУиРпШЧхмКЧжх', 'рвЩЗоЗхшЗвлизкСзебЩКМКжбша', 'ДииБНСШвцЦбаСсИжЕЗмхмВ', 'СЦоБЗПМтмшрУлрДТФГЖиувШЗууШзв', 'ЦЗБЕзВХЙбйВОмЗпхндЗ', 'ЗНизЧВШкГВтпсЖж', 'уШиБПЙЧтРаЕгИ', 'ЙшпПА', 'ЧоММаАйМСфбхуФкефФштгУА']) from system.numbers limit 10;

select 0 = multiSearchFirstPosition(materialize('abcdefgh'), ['z', 'pq']) from system.numbers limit 10;
select 1 = multiSearchFirstPosition(materialize('abcdefgh'), ['a', 'b', 'c', 'd']) from system.numbers limit 10;
select 1 = multiSearchFirstPosition(materialize('abcdefgh'), ['defgh', 'bcd', 'abcd', 'c']) from system.numbers limit 10;
select 1 = multiSearchFirstPosition(materialize('abcdefgh'), ['', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 2 = multiSearchFirstPosition(materialize('abcdefgh'), ['something', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 6 = multiSearchFirstPosition(materialize('abcdefgh'), ['something', 'bcdz', 'fgh', 'f']) from system.numbers limit 10;

select 0 = multiSearchFirstPositionCaseInsensitive(materialize('abcdefgh'), ['z', 'pq']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitive(materialize('aBcdefgh'), ['A', 'b', 'c', 'd']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitive(materialize('abCDefgh'), ['defgh', 'bcd', 'aBCd', 'c']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitive(materialize('abCdeFgH'), ['', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 2 = multiSearchFirstPositionCaseInsensitive(materialize('ABCDEFGH'), ['something', 'bcd', 'bcd', 'c']) from system.numbers limit 10;
select 6 = multiSearchFirstPositionCaseInsensitive(materialize('abcdefgh'), ['sOmEthIng', 'bcdZ', 'fGh', 'F']) from system.numbers limit 10;

select 0 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['л', 'ъ']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['а', 'б', 'в', 'г']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['гдежз', 'бвг', 'абвг', 'вг']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['', 'бвг', 'бвг', 'в']) from system.numbers limit 10;
select 2 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['что', 'в', 'гдз', 'бвг']) from system.numbers limit 10;
select 6 = multiSearchFirstPositionUTF8(materialize('абвгдежз'), ['з', 'бвгя', 'ежз', 'з']) from system.numbers limit 10;

select 0 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['Л', 'Ъ']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['А', 'б', 'в', 'г']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['гДеЖз', 'бВг', 'АБВг', 'вг']) from system.numbers limit 10;
select 1 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['', 'бвг', 'Бвг', 'в']) from system.numbers limit 10;
select 2 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежз'), ['что', 'в', 'гдз', 'бвг']) from system.numbers limit 10;
select 6 = multiSearchFirstPositionCaseInsensitiveUTF8(materialize('аБвгДежЗ'), ['З', 'бвгЯ', 'ЕЖз', 'з']) from system.numbers limit 10;

select 1 = multiSearchAny(materialize('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
            ['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'a']);

select 0 = multiSearchAny(materialize('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
            ['aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab',
'b']);

-- 254
select
[
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
] =
multiSearchAllPositions(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);

select 254 = multiSearchFirstIndex(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);


select
[
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
] =
multiSearchAllPositions(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);

select 255 = multiSearchFirstIndex(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']);

select multiSearchAllPositions(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']); -- { serverError 42 }

select multiSearchFirstIndex(materialize('string'),
['o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o',
'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'o', 'str']); -- { serverError 42 }
