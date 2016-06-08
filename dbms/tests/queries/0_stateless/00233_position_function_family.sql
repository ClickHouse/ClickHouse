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
