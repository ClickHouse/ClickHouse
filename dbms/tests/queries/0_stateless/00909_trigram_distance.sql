select round(1000 * distance(materialize(''), '')) from system.numbers limit 5;
select round(1000 * distance(materialize('абв'), '')) from system.numbers limit 5;
select round(1000 * distance(materialize(''), 'абв')) from system.numbers limit 5;
select round(1000 * distance(materialize('абвгдеёжз'), 'абвгдеёжз')) from system.numbers limit 5;
select round(1000 * distance(materialize('абвгдеёжз'), 'абвгдеёж')) from system.numbers limit 5;
select round(1000 * distance(materialize('абвгдеёжз'), 'гдеёзд')) from system.numbers limit 5;
select round(1000 * distance(materialize('абвгдеёжз'), 'ёёёёёёёё')) from system.numbers limit 5;

select round(1000 * distance('', ''));
select round(1000 * distance('абв', ''));
select round(1000 * distance('', 'абв'));
select round(1000 * distance('абвгдеёжз', 'абвгдеёжз'));
select round(1000 * distance('абвгдеёжз', 'абвгдеёж'));
select round(1000 * distance('абвгдеёжз', 'гдеёзд'));
select round(1000 * distance('абвгдеёжз', 'ёёёёёёёё'));

drop table if exists test.test_distance;
create table test.test_distance (Title String) engine = Memory;
insert into test.test_distance values ('привет как дела?... Херсон'), ('привет как дела клип - Яндекс.Видео'), ('привет'), ('пап привет как дела - Яндекс.Видео'), ('привет братан как дела - Яндекс.Видео'), ('http://metric.ru/'), ('http://autometric.ru/'), ('http://metrica.yandex.com/'), ('http://metris.ru/'), ('http://metrika.ru/'), ('');

SELECT Title FROM test.test_distance ORDER BY distance(Title, 'привет как дела');
SELECT Title FROM test.test_distance ORDER BY distance(Title, 'как привет дела');
SELECT Title FROM test.test_distance ORDER BY distance(Title, 'metrika');
SELECT Title FROM test.test_distance ORDER BY distance(Title, 'metrica');
SELECT Title FROM test.test_distance ORDER BY distance(Title, 'metriks');
SELECT Title FROM test.test_distance ORDER BY distance(Title, 'metrics');
SELECT Title FROM test.test_distance ORDER BY distance(Title, 'yandex');

drop table if exists test.test_distance;
