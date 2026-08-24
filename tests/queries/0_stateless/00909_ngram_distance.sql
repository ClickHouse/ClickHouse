select round(1000 * ngramDistanceUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramDistanceUTF8(materialize(''), materialize('')))=round(1000 * ngramDistanceUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абв'), materialize('')))=round(1000 * ngramDistanceUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize(''), materialize('абв')))=round(1000 * ngramDistanceUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), materialize('абвгдеёжз')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), materialize('абвгдеёж')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), materialize('гдеёзд')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), materialize('ёёёёёёёё')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramDistanceUTF8('', materialize('')))=round(1000 * ngramDistanceUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8('абв', materialize('')))=round(1000 * ngramDistanceUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8('', materialize('абв')))=round(1000 * ngramDistanceUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8('абвгдеёжз', materialize('абвгдеёжз')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8('абвгдеёжз', materialize('абвгдеёж')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8('абвгдеёжз', materialize('гдеёзд')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * ngramDistanceUTF8('абвгдеёжз', materialize('ёёёёёёёё')))=round(1000 * ngramDistanceUTF8(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * ngramDistanceUTF8('', ''));
select round(1000 * ngramDistanceUTF8('абв', ''));
select round(1000 * ngramDistanceUTF8('', 'абв'));
select round(1000 * ngramDistanceUTF8('абвгдеёжз', 'абвгдеёжз'));
select round(1000 * ngramDistanceUTF8('абвгдеёжз', 'абвгдеёж'));
select round(1000 * ngramDistanceUTF8('абвгдеёжз', 'гдеёзд'));
select round(1000 * ngramDistanceUTF8('абвгдеёжз', 'ёёёёёёёё'));

drop table if exists test_distance;
create table test_distance (Title String) engine = Memory;
insert into test_distance values ('привет как дела?... Херсон'), ('привет как дела клип - TUT.BY'), ('привет'), ('пап привет как дела - TUT.BY'), ('привет братан как дела - TUT.BY'), ('http://metric.ru/'), ('http://autometric.ru/'), ('http://top.bigmir.net/'), ('http://metris.ru/'), ('http://metrika.ru/'), ('');

SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, Title) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, extract(Title, 'как дела')) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, extract(Title, 'metr')) as distance, Title;

SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'привет как дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'как привет дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'metrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'metriks') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceUTF8(Title, 'bigmir') as distance, Title;


select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''),materialize(''))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абв'),materialize(''))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), materialize('абв'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвГДЕёжз'), materialize('АбвгдЕёжз'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), materialize('АбвГдеёж'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), materialize('гдеёЗД'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;

select round(1000 * ngramDistanceCaseInsensitiveUTF8('', materialize(''))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абв',materialize(''))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8('', materialize('абв'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абвГДЕёжз', materialize('АбвгдЕёжз'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвГДЕёжз'), 'АбвгдЕёжз')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8('аБВГдеёЖз', materialize('АбвГдеёж'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('аБВГдеёЖз'), 'АбвГдеёж')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абвгдеёжз', materialize('гдеёЗД'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'гдеёЗД')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абвгдеёжз', materialize('ЁЁЁЁЁЁЁЁ'))) = round(1000 * ngramDistanceCaseInsensitiveUTF8(materialize('абвгдеёжз'), 'ЁЁЁЁЁЁЁЁ')) from system.numbers limit 5;


select round(1000 * ngramDistanceCaseInsensitiveUTF8('', ''));
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абв', ''));
select round(1000 * ngramDistanceCaseInsensitiveUTF8('', 'абв'));
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абвГДЕёжз', 'АбвгдЕЁжз'));
select round(1000 * ngramDistanceCaseInsensitiveUTF8('аБВГдеёЖз', 'АбвГдеёж'));
select round(1000 * ngramDistanceCaseInsensitiveUTF8('абвгдеёжз', 'гдеёЗД'));
select round(1000 * ngramDistanceCaseInsensitiveUTF8('АБВГДеёжз', 'ЁЁЁЁЁЁЁЁ'));

SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, Title) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, extract(Title, 'как дела')) as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, extract(Title, 'metr')) as distance, Title;

SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'ПрИвЕт кАК ДЕЛа') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'как ПРИВЕТ дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'Metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'mEtrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'metriKS') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'BigMIR') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitiveUTF8(Title, 'приВЕТ КАк ДеЛа КлИп - TuT.by') as distance, Title;


select round(1000 * ngramDistance(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramDistance(materialize(''),materialize('')))=round(1000 * ngramDistance(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abc'),materialize('')))=round(1000 * ngramDistance(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize(''), materialize('abc')))=round(1000 * ngramDistance(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), materialize('abcdefgh')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), materialize('abcdefg')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), materialize('defgh')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramDistance(materialize('abcdefgh'), materialize('aaaaaaaa')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramDistance('',materialize('')))=round(1000 * ngramDistance(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistance('abc', materialize('')))=round(1000 * ngramDistance(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramDistance('', materialize('abc')))=round(1000 * ngramDistance(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramDistance('abcdefgh', materialize('abcdefgh')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramDistance('abcdefgh', materialize('abcdefg')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'abcdefg')) from system.numbers limit 5;
select round(1000 * ngramDistance('abcdefgh', materialize('defgh')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramDistance('abcdefgh', materialize('aaaaaaaa')))=round(1000 * ngramDistance(materialize('abcdefgh'), 'aaaaaaaa')) from system.numbers limit 5;


select round(1000 * ngramDistance('', ''));
select round(1000 * ngramDistance('abc', ''));
select round(1000 * ngramDistance('', 'abc'));
select round(1000 * ngramDistance('abcdefgh', 'abcdefgh'));
select round(1000 * ngramDistance('abcdefgh', 'abcdefg'));
select round(1000 * ngramDistance('abcdefgh', 'defgh'));
select round(1000 * ngramDistance('abcdefgh', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'привет как дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'как привет дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'metrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'metriks') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistance(Title, 'bigmir') as distance, Title;

select round(1000 * ngramDistanceCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramDistanceCaseInsensitive(materialize(''), materialize('')))=round(1000 * ngramDistanceCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('abc'), materialize('')))=round(1000 * ngramDistanceCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize(''), materialize('abc')))=round(1000 * ngramDistanceCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('abCdefgH'), materialize('Abcdefgh')))=round(1000 * ngramDistanceCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('abcdefgh'), materialize('abcdeFG')))=round(1000 * ngramDistanceCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('AAAAbcdefgh'), materialize('defgh')))=round(1000 * ngramDistanceCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive(materialize('ABCdefgH'), materialize('aaaaaaaa')))=round(1000 * ngramDistanceCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramDistanceCaseInsensitive('', materialize('')))=round(1000 * ngramDistanceCaseInsensitive(materialize(''), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive('abc', materialize('')))=round(1000 * ngramDistanceCaseInsensitive(materialize('abc'), '')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive('', materialize('abc')))=round(1000 * ngramDistanceCaseInsensitive(materialize(''), 'abc')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive('abCdefgH', materialize('Abcdefgh')))=round(1000 * ngramDistanceCaseInsensitive(materialize('abCdefgH'), 'Abcdefgh')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive('abcdefgh', materialize('abcdeFG')))=round(1000 * ngramDistanceCaseInsensitive(materialize('abcdefgh'), 'abcdeFG')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive('AAAAbcdefgh', materialize('defgh')))=round(1000 * ngramDistanceCaseInsensitive(materialize('AAAAbcdefgh'), 'defgh')) from system.numbers limit 5;
select round(1000 * ngramDistanceCaseInsensitive('ABCdefgH', materialize('aaaaaaaa')))=round(1000 * ngramDistanceCaseInsensitive(materialize('ABCdefgH'), 'aaaaaaaa')) from system.numbers limit 5;

select round(1000 * ngramDistanceCaseInsensitive('', ''));
select round(1000 * ngramDistanceCaseInsensitive('abc', ''));
select round(1000 * ngramDistanceCaseInsensitive('', 'abc'));
select round(1000 * ngramDistanceCaseInsensitive('abCdefgH', 'Abcdefgh'));
select round(1000 * ngramDistanceCaseInsensitive('abcdefgh', 'abcdeFG'));
select round(1000 * ngramDistanceCaseInsensitive('AAAAbcdefgh', 'defgh'));
select round(1000 * ngramDistanceCaseInsensitive('ABCdefgH', 'aaaaaaaa'));

SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'ПрИвЕт кАК ДЕЛа') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'как ПРИВЕТ дела') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'Metrika') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'mEtrica') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'metriKS') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'metrics') as distance, Title;
SELECT Title, round(1000 * distance) FROM test_distance ORDER BY ngramDistanceCaseInsensitive(Title, 'BigMIR') as distance, Title;

drop table if exists test_distance;
