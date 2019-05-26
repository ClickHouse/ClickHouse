select round(1000 * ngramEntryUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramEntryUTF8(materialize(''), materialize('')))=round(1000 * ngramEntryUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абв'), materialize('')))=round(1000 * ngramEntryUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize(''), materialize('абв')))=round(1000 * ngramEntryUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), materialize('абвгдеёжз')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), materialize('абвгдеёж')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), materialize('гдеёзд')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), materialize('ёёёёёёёё')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramEntryUTF8('', materialize('')))=round(1000 * ngramEntryUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8('абв', materialize('')))=round(1000 * ngramEntryUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8('', materialize('абв')))=round(1000 * ngramEntryUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8('абвгдеёжз', materialize('абвгдеёжз')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8('абвгдеёжз', materialize('абвгдеёж')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8('абвгдеёжз', materialize('гдеёзд')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramEntryUTF8('абвгдеёжз', materialize('ёёёёёёёё')))=round(1000 * ngramEntryUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramEntryUTF8('', ''));
select round(1000 * ngramEntryUTF8('абв', ''));
select round(1000 * ngramEntryUTF8('', 'абв'));
select round(1000 * ngramEntryUTF8('абвгдеёжз', 'абвгдеёжз'));
select round(1000 * ngramEntryUTF8('абвгдеёжз', 'абвгдеёж'));
select round(1000 * ngramEntryUTF8('абвгдеёжз', 'гдеёзд'));
select round(1000 * ngramEntryUTF8('абвгдеёжз', 'ёёёёёёёё'));

drop table if exists test_entry_distance;
create table test_entry_distance (Title String) engine = Memory;
insert into test_entry_distance values ('привет как дела?... Херсон'), ('привет как дела клип - Яндекс.Видео'), ('привет'), ('пап привет как дела - Яндекс.Видео'), ('привет братан как дела - Яндекс.Видео'), ('http://metric.ru/'), ('http://autometric.ru/'), ('http://metrica.yandex.com/'), ('http://metris.ru/'), ('http://metrika.ru/'), ('');

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, Title) as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, extract(Title, 'как дела')) as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, extract(Title, 'metr')) as distance;

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'привет как дела') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'как привет дела') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'metrika') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'metrica') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'metriks') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'metrics') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryUTF8(Title, 'yandex') as distance;


select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''),materialize(''))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абв'),materialize(''))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), materialize('абв'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвГДЕёжз'), materialize('АбвгдЕёжз'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), materialize('АбвГдеёж'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), materialize('гдеёЗД'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngramEntryCaseInsensitiveUTF8('', materialize(''))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8('абв',materialize(''))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8('', materialize('абв'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8('абвГДЕёжз', materialize('АбвгдЕёжз'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8('аБВГдеёЖз', materialize('АбвГдеёж'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8('абвгдеёжз', materialize('гдеёЗД'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitiveUTF8('абвгдеёжз', materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngramEntryCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;


select round(1000 * ngramEntryCaseInsensitiveUTF8('', ''));
select round(1000 * ngramEntryCaseInsensitiveUTF8('абв', ''));
select round(1000 * ngramEntryCaseInsensitiveUTF8('', 'абв'));
select round(1000 * ngramEntryCaseInsensitiveUTF8('абвГДЕёжз', 'АбвгдЕЁжз'));
select round(1000 * ngramEntryCaseInsensitiveUTF8('аБВГдеёЖз', 'АбвГдеёж'));
select round(1000 * ngramEntryCaseInsensitiveUTF8('абвгдеёжз', 'гдеёЗД'));
select round(1000 * ngramEntryCaseInsensitiveUTF8('АБВГДеёжз', 'ЁЁЁЁЁЁЁЁ'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, Title) as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, extract(Title, 'как дела')) as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, extract(Title, 'metr')) as distance;

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'ПрИвЕт кАК ДЕЛа') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'как ПРИВЕТ дела') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'metrika') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'Metrika') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'mEtrica') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'metriKS') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'metrics') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'YanDEX') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitiveUTF8(Title, 'приВЕТ КАк ДеЛа КлИп - яндеКс.видео') as distance;


select round(1000 * ngramEntry(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramEntry(materialize(''),materialize('')))=round(1000 * ngramEntry(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abc'),materialize('')))=round(1000 * ngramEntry(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize(''), materialize('abc')))=round(1000 * ngramEntry(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), materialize('abcdefgh')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), materialize('abcdefg')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), materialize('defgh')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramEntry(materialize('abcdefgh'), materialize('aaaaaaaa')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramEntry('',materialize('')))=round(1000 * ngramEntry(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntry('abc', materialize('')))=round(1000 * ngramEntry(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramEntry('', materialize('abc')))=round(1000 * ngramEntry(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramEntry('abcdefgh', materialize('abcdefgh')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramEntry('abcdefgh', materialize('abcdefg')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramEntry('abcdefgh', materialize('defgh')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramEntry('abcdefgh', materialize('aaaaaaaa')))=round(1000 * ngramEntry(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;


select round(1000 * ngramEntry('', ''));
select round(1000 * ngramEntry('abc', ''));
select round(1000 * ngramEntry('', 'abc'));
select round(1000 * ngramEntry('abcdefgh', 'abcdefgh'));
select round(1000 * ngramEntry('abcdefgh', 'abcdefg'));
select round(1000 * ngramEntry('abcdefgh', 'defgh'));
select round(1000 * ngramEntry('abcdefghaaaaaaaaaa', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'привет как дела') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'как привет дела') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'metrika') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'metrica') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'metriks') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'metrics') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntry(Title, 'yandex') as distance;

select round(1000 * ngramEntryCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramEntryCaseInsensitive(materialize(''), materialize('')))=round(1000 * ngramEntryCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('abc'), materialize('')))=round(1000 * ngramEntryCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize(''), materialize('abc')))=round(1000 * ngramEntryCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('abCdefgH'), materialize('Abcdefgh')))=round(1000 * ngramEntryCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('abcdefgh'), materialize('abcdeFG')))=round(1000 * ngramEntryCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('AAAAbcdefgh'), materialize('defgh')))=round(1000 * ngramEntryCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive(materialize('ABCdefgH'), materialize('aaaaaaaa')))=round(1000 * ngramEntryCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramEntryCaseInsensitive('', materialize('')))=round(1000 * ngramEntryCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive('abc', materialize('')))=round(1000 * ngramEntryCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive('', materialize('abc')))=round(1000 * ngramEntryCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive('abCdefgH', materialize('Abcdefgh')))=round(1000 * ngramEntryCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive('abcdefgh', materialize('abcdeFG')))=round(1000 * ngramEntryCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive('AAAAbcdefgh', materialize('defgh')))=round(1000 * ngramEntryCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramEntryCaseInsensitive('ABCdefgH', materialize('aaaaaaaa')))=round(1000 * ngramEntryCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramEntryCaseInsensitive('', ''));
select round(1000 * ngramEntryCaseInsensitive('abc', ''));
select round(1000 * ngramEntryCaseInsensitive('', 'abc'));
select round(1000 * ngramEntryCaseInsensitive('abCdefgH', 'Abcdefgh'));
select round(1000 * ngramEntryCaseInsensitive('abcdefgh', 'abcdeFG'));
select round(1000 * ngramEntryCaseInsensitive('AAAAbcdefgh', 'defgh'));
select round(1000 * ngramEntryCaseInsensitive('ABCdefgHaAaaaAaaaAA', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'ПрИвЕт кАК ДЕЛа') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'как ПРИВЕТ дела') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'metrika') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'Metrika') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'mEtrica') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'metriKS') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'metrics') as distance;
SELECT Title, round(1000 * distance) FROM test_entry_distance ORDER BY ngramEntryCaseInsensitive(Title, 'YanDEX') as distance;

drop table if exists test_entry_distance;
