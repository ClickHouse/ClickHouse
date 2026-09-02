select round(1000 * ngramSearchUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramSearchUTF8(materialize(''), materialize('')))=round(1000 * ngramSearchUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абв'), materialize('')))=round(1000 * ngramSearchUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize(''), materialize('абв')))=round(1000 * ngramSearchUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), materialize('абвгдеёжз')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), materialize('абвгдеёж')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), materialize('гдеёзд')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), materialize('ёёёёёёёё')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramSearchUTF8('', materialize('')))=round(1000 * ngramSearchUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8('абв', materialize('')))=round(1000 * ngramSearchUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8('', materialize('абв')))=round(1000 * ngramSearchUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8('абвгдеёжз', materialize('абвгдеёжз')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8('абвгдеёжз', materialize('абвгдеёж')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8('абвгдеёжз', materialize('гдеёзд')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramSearchUTF8('абвгдеёжз', materialize('ёёёёёёёё')))=round(1000 * ngramSearchUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramSearchUTF8('', ''));
select round(1000 * ngramSearchUTF8('абв', ''));
select round(1000 * ngramSearchUTF8('', 'абв'));
select round(1000 * ngramSearchUTF8('абвгдеёжз', 'абвгдеёжз'));
select round(1000 * ngramSearchUTF8('абвгдеёжз', 'абвгдеёж'));
select round(1000 * ngramSearchUTF8('абвгдеёжз', 'гдеёзд'));
select round(1000 * ngramSearchUTF8('абвгдеёжз', 'ёёёёёёёё'));

drop table if exists test_entry_distance;
create table test_entry_distance (Title String) engine = Memory;
insert into test_entry_distance values ('привет как дела?... Херсон'), ('привет как дела клип - TUT.BY'), ('привет'), ('пап привет как дела - TUT.BY'), ('привет братан как дела - TUT.BY'), ('http://metric.ru/'), ('http://autometric.ru/'), ('http://top.bigmir.net/'), ('http://metris.ru/'), ('http://metrika.ru/'), ('');

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, Title) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, extract(Title, 'как дела')) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, extract(Title, 'metr')) as distance, Title;

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'привет как дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'как привет дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'metrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'metriks') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchUTF8(Title, 'bigmir') as distance, Title;


select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''),materialize(''))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абв'),materialize(''))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), materialize('абв'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвГДЕёжз'), materialize('АбвгдЕёжз'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), materialize('АбвГдеёж'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), materialize('гдеёЗД'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngramSearchCaseInsensitiveUTF8('', materialize(''))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8('абв',materialize(''))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8('', materialize('абв'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8('абвГДЕёжз', materialize('АбвгдЕёжз'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8('аБВГдеёЖз', materialize('АбвГдеёж'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8('абвгдеёжз', materialize('гдеёЗД'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitiveUTF8('абвгдеёжз', materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngramSearchCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;


select round(1000 * ngramSearchCaseInsensitiveUTF8('', ''));
select round(1000 * ngramSearchCaseInsensitiveUTF8('абв', ''));
select round(1000 * ngramSearchCaseInsensitiveUTF8('', 'абв'));
select round(1000 * ngramSearchCaseInsensitiveUTF8('абвГДЕёжз', 'АбвгдЕЁжз'));
select round(1000 * ngramSearchCaseInsensitiveUTF8('аБВГдеёЖз', 'АбвГдеёж'));
select round(1000 * ngramSearchCaseInsensitiveUTF8('абвгдеёжз', 'гдеёЗД'));
select round(1000 * ngramSearchCaseInsensitiveUTF8('АБВГДеёжз', 'ЁЁЁЁЁЁЁЁ'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, Title) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, extract(Title, 'как дела')) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, extract(Title, 'metr')) as distance, Title;

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'ПрИвЕт кАК ДЕЛа') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'как ПРИВЕТ дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'Metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'mEtrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'metriKS') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'BigMIR') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitiveUTF8(Title, 'приВЕТ КАк ДеЛа КлИп - bigMir.Net') as distance, Title;


select round(1000 * ngramSearch(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramSearch(materialize(''),materialize('')))=round(1000 * ngramSearch(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abc'),materialize('')))=round(1000 * ngramSearch(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize(''), materialize('abc')))=round(1000 * ngramSearch(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), materialize('abcdefgh')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), materialize('abcdefg')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), materialize('defgh')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramSearch(materialize('abcdefgh'), materialize('aaaaaaaa')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramSearch('',materialize('')))=round(1000 * ngramSearch(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearch('abc', materialize('')))=round(1000 * ngramSearch(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramSearch('', materialize('abc')))=round(1000 * ngramSearch(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramSearch('abcdefgh', materialize('abcdefgh')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramSearch('abcdefgh', materialize('abcdefg')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramSearch('abcdefgh', materialize('defgh')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramSearch('abcdefgh', materialize('aaaaaaaa')))=round(1000 * ngramSearch(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;


select round(1000 * ngramSearch('', ''));
select round(1000 * ngramSearch('abc', ''));
select round(1000 * ngramSearch('', 'abc'));
select round(1000 * ngramSearch('abcdefgh', 'abcdefgh'));
select round(1000 * ngramSearch('abcdefgh', 'abcdefg'));
select round(1000 * ngramSearch('abcdefgh', 'defgh'));
select round(1000 * ngramSearch('abcdefghaaaaaaaaaa', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'привет как дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'как привет дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'metrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'metriks') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearch(Title, 'bigmir') as distance, Title;

select round(1000 * ngramSearchCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramSearchCaseInsensitive(materialize(''), materialize('')))=round(1000 * ngramSearchCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('abc'), materialize('')))=round(1000 * ngramSearchCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize(''), materialize('abc')))=round(1000 * ngramSearchCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('abCdefgH'), materialize('Abcdefgh')))=round(1000 * ngramSearchCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('abcdefgh'), materialize('abcdeFG')))=round(1000 * ngramSearchCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('AAAAbcdefgh'), materialize('defgh')))=round(1000 * ngramSearchCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive(materialize('ABCdefgH'), materialize('aaaaaaaa')))=round(1000 * ngramSearchCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramSearchCaseInsensitive('', materialize('')))=round(1000 * ngramSearchCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive('abc', materialize('')))=round(1000 * ngramSearchCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive('', materialize('abc')))=round(1000 * ngramSearchCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive('abCdefgH', materialize('Abcdefgh')))=round(1000 * ngramSearchCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive('abcdefgh', materialize('abcdeFG')))=round(1000 * ngramSearchCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive('AAAAbcdefgh', materialize('defgh')))=round(1000 * ngramSearchCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramSearchCaseInsensitive('ABCdefgH', materialize('aaaaaaaa')))=round(1000 * ngramSearchCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramSearchCaseInsensitive('', ''));
select round(1000 * ngramSearchCaseInsensitive('abc', ''));
select round(1000 * ngramSearchCaseInsensitive('', 'abc'));
select round(1000 * ngramSearchCaseInsensitive('abCdefgH', 'Abcdefgh'));
select round(1000 * ngramSearchCaseInsensitive('abcdefgh', 'abcdeFG'));
select round(1000 * ngramSearchCaseInsensitive('AAAAbcdefgh', 'defgh'));
select round(1000 * ngramSearchCaseInsensitive('ABCdefgHaAaaaAaaaAA', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'ПрИвЕт кАК ДЕЛа') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'как ПРИВЕТ дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'Metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'mEtrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'metriKS') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramSearchCaseInsensitive(Title, 'BigMIR') as distance, Title;

drop table if exists test_entry_distance;
