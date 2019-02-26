select round(1000 * trigramDistance(materialize(''), '')) from system.numbers limit 5;
select round(1000 * trigramDistance(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * trigramDistance(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * trigramDistance(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * trigramDistance(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * trigramDistance(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * trigramDistance(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * trigramDistance('', ''));
select round(1000 * trigramDistance('абв', ''));
select round(1000 * trigramDistance('', 'абв'));
select round(1000 * trigramDistance('абвгдеёжз', 'абвгдеёжз'));
select round(1000 * trigramDistance('абвгдеёжз', 'абвгдеёж'));
select round(1000 * trigramDistance('абвгдеёжз', 'гдеёзд'));
select round(1000 * trigramDistance('абвгдеёжз', 'ёёёёёёёё'));

drop table if exists test.test_distance;
create table test.test_distance (Title String) engine = Memory;
insert into test.test_distance values ('привет как дела?... Херсон'), ('привет как дела клип - Яндекс.Видео'), ('привет'), ('пап привет как дела - Яндекс.Видео'), ('привет братан как дела - Яндекс.Видео'), ('http://metric.ru/'), ('http://autometric.ru/'), ('http://metrica.yandex.com/'), ('http://metris.ru/'), ('http://metrika.ru/'), ('');

SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'привет как дела');
SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'как привет дела');
SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'metrika');
SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'metrica');
SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'metriks');
SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'metrics');
SELECT Title FROM test.test_distance ORDER BY trigramDistance(Title, 'yandex');

drop table if exists test.test_distance;
