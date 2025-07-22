-- Tags: no-fasttest
-- no-fasttest: upper/lowerUTF8 use ICU

--
-- countSubstrings
--
select '';
select '# countSubstrings';

select '';
select 'CountSubstringsImpl::constantConstant';
select 'CountSubstringsImpl::constantConstantScalar';

select 'empty';
select countSubstrings('', '.');
select countSubstrings('', '');
select countSubstrings('.', '');
select countSubstrings(toString(number), '') from numbers(1);
select countSubstrings('', toString(number)) from numbers(1);
select countSubstrings('aaa', materialize(''));
select countSubstrings(materialize('aaa'), '');
select countSubstrings(materialize('aaa'), materialize(''));

select 'char';
select countSubstrings('foobar.com', '.');
select countSubstrings('www.foobar.com', '.');
select countSubstrings('.foobar.com.', '.');

select 'word';
select countSubstrings('foobar.com', 'com');
select countSubstrings('com.foobar', 'com');
select countSubstrings('foo.com.bar', 'com');
select countSubstrings('com.foobar.com', 'com');
select countSubstrings('com.foo.com.bar.com', 'com');

select 'intersect';
select countSubstrings('aaaa', 'aa');

select '';
select 'CountSubstringsImpl::vectorVector';
select countSubstrings(toString(number), toString(number)) from numbers(1);
select countSubstrings(concat(toString(number), '000111'), toString(number)) from numbers(1);
select countSubstrings(concat(toString(number), '000111001'), toString(number)) from numbers(1);
select 'intersect', countSubstrings(concat(toString(number), '0000000'), '00') from numbers(1) format CSV;

select '';
select 'CountSubstringsImpl::constantVector';
select countSubstrings('100', toString(number)) from numbers(3);
select countSubstrings('0100', toString(number)) from numbers(1);
select countSubstrings('010000', toString(number)) from numbers(1);
select 'intersect', countSubstrings('00000000', repeat(toString(number), 2)) from numbers(1) format CSV;

select '';
select 'CountSubstringsImpl::vectorConstant';
select countSubstrings(toString(number), '1') from system.numbers limit 3 offset 9;
select countSubstrings(concat(toString(number), '000111'), '1') from numbers(1);
select countSubstrings(concat(toString(number), '000111001'), '1') from numbers(1);
select 'intersect', countSubstrings(repeat(toString(number), 8), '00') from numbers(1) format CSV;

--
-- countSubstringsCaseInsensitive
--
select '';
select '# countSubstringsCaseInsensitive';

select '';
select 'CountSubstringsImpl::constantConstant';
select 'CountSubstringsImpl::constantConstantScalar';

select 'char';
select countSubstringsCaseInsensitive('aba', 'B');
select countSubstringsCaseInsensitive('bab', 'B');
select countSubstringsCaseInsensitive('BaBaB', 'b');

select 'word';
select countSubstringsCaseInsensitive('foobar.com', 'COM');
select countSubstringsCaseInsensitive('com.foobar', 'COM');
select countSubstringsCaseInsensitive('foo.com.bar', 'COM');
select countSubstringsCaseInsensitive('com.foobar.com', 'COM');
select countSubstringsCaseInsensitive('com.foo.com.bar.com', 'COM');

select 'intersect';
select countSubstringsCaseInsensitive('aaaa', 'AA');

select '';
select 'CountSubstringsImpl::vectorVector';
select countSubstringsCaseInsensitive(upper(char(number)), lower(char(number))) from numbers(100) where number = 0x41; -- A
select countSubstringsCaseInsensitive(concat(toString(number), 'aaa111'), char(number)) from numbers(100) where number = 0x41;
select countSubstringsCaseInsensitive(concat(toString(number), 'aaa111aa1'), char(number)) from numbers(100) where number = 0x41;

select '';
select 'CountSubstringsImpl::constantVector';
select countSubstringsCaseInsensitive('aab', char(number)) from numbers(100) where number >= 0x41 and number <= 0x43; -- A..C
select countSubstringsCaseInsensitive('abaa', char(number)) from numbers(100) where number = 0x41;
select countSubstringsCaseInsensitive('abaaaa', char(number)) from numbers(100) where number = 0x41;

select '';
select 'CountSubstringsImpl::vectorConstant';
select countSubstringsCaseInsensitive(char(number), 'a') from numbers(100) where number >= 0x41 and number <= 0x43;

--
-- countSubstringsCaseInsensitiveUTF8
--
select '';
select '# countSubstringsCaseInsensitiveUTF8';

select '';
select 'CountSubstringsImpl::constantConstant';
select 'CountSubstringsImpl::constantConstantScalar';

select 'char';
select countSubstringsCaseInsensitiveUTF8('фуу', 'Ф');
select countSubstringsCaseInsensitiveUTF8('ФуФ', 'ф');
select countSubstringsCaseInsensitiveUTF8('ФуФуФ', 'ф');

select 'word';
select countSubstringsCaseInsensitiveUTF8('подстрока.рф', 'РФ');
select countSubstringsCaseInsensitiveUTF8('рф.подстрока', 'рф');
select countSubstringsCaseInsensitiveUTF8('подстрока.рф.подстрока', 'РФ');
select countSubstringsCaseInsensitiveUTF8('рф.подстрока.рф', 'рф');
select countSubstringsCaseInsensitiveUTF8('рф.подстрока.рф.подстрока.рф', 'РФ');

select 'intersect';
select countSubstringsCaseInsensitiveUTF8('яяяя', 'ЯЯ');

select '';
select 'CountSubstringsImpl::vectorVector';
-- can't use any char, since this will not make valid UTF8
-- for the haystack we use number as-is, for needle we just add dependency from number to go to vectorVector code
select countSubstringsCaseInsensitiveUTF8(upperUTF8(concat(char(number), 'я')), lowerUTF8(concat(substringUTF8(char(number), 2), 'Я'))) from numbers(100) where number = 0x41; -- A
select countSubstringsCaseInsensitiveUTF8(concat(toString(number), 'ЯЯЯ111'), concat(substringUTF8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select countSubstringsCaseInsensitiveUTF8(concat(toString(number), 'яяя111яя1'), concat(substringUTF8(char(number), 2), 'Я')) from numbers(100) where number = 0x41; -- A
select 'intersect', countSubstringsCaseInsensitiveUTF8(concat(toString(number), 'яяяяяяяя'), concat(substringUTF8(char(number), 2), 'Яя')) from numbers(100) where number = 0x41 format CSV; -- A

select '';
select 'CountSubstringsImpl::constantVector';
select countSubstringsCaseInsensitiveUTF8('ЯЯb', concat(substringUTF8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select countSubstringsCaseInsensitiveUTF8('ЯbЯЯ', concat(substringUTF8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select countSubstringsCaseInsensitiveUTF8('ЯbЯЯЯЯ', concat(substringUTF8(char(number), 2), 'я')) from numbers(100) where number = 0x41; -- A
select 'intersect', countSubstringsCaseInsensitiveUTF8('ЯЯЯЯЯЯЯЯ', concat(substringUTF8(char(number), 2), 'Яя')) from numbers(100) where number = 0x41 format CSV; -- A

select '';
select 'CountSubstringsImpl::vectorConstant';
select countSubstringsCaseInsensitiveUTF8(concat(char(number), 'я'), 'Я') from numbers(100) where number = 0x41; -- A
select countSubstringsCaseInsensitiveUTF8(concat(char(number), 'б'), 'Я') from numbers(100) where number = 0x41; -- A
select 'intersect', countSubstringsCaseInsensitiveUTF8(concat(char(number), repeat('я', 8)), 'яЯ') from numbers(100) where number = 0x41 format CSV; -- A
