select 'hasSubsequence';
select hasSubsequence('garbage', '');
select hasSubsequence('garbage', 'g');
select hasSubsequence('garbage', 'G');
select hasSubsequence('garbage', 'a');
select hasSubsequence('garbage', 'e');
select hasSubsequence('garbage', 'gr');
select hasSubsequence('garbage', 'ab');
select hasSubsequence('garbage', 'be');
select hasSubsequence('garbage', 'arg');
select hasSubsequence('garbage', 'gra');
select hasSubsequence('garbage', 'rga');
select hasSubsequence('garbage', 'garbage');
select hasSubsequence('garbage', 'garbage1');
select hasSubsequence('garbage', 'arbw');
select hasSubsequence('garbage', 'ARG');
select hasSubsequence('garbage', materialize(''));
select hasSubsequence('garbage', materialize('arg'));
select hasSubsequence('garbage', materialize('arbw'));
select hasSubsequence(materialize('garbage'), '');
select hasSubsequence(materialize('garbage'), 'arg');
select hasSubsequence(materialize('garbage'), 'arbw');
select hasSubsequence(materialize('garbage'), materialize(''));
select hasSubsequence(materialize('garbage'), materialize('arg'));
select hasSubsequence(materialize('garbage'), materialize('garbage1'));

select 'hasSubsequenceCaseInsensitive';
select hasSubsequenceCaseInsensitive('garbage', 'w');
select hasSubsequenceCaseInsensitive('garbage', 'ARG');
select hasSubsequenceCaseInsensitive('GARGAGE', 'arg');
select hasSubsequenceCaseInsensitive(materialize('garbage'), materialize('w'));
select hasSubsequenceCaseInsensitive(materialize('garbage'), materialize('ARG'));
select hasSubsequenceCaseInsensitive(materialize('GARGAGE'), materialize('arg'));

select 'hasSubsequenceUTF8';
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', '');
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'C');     -- eng
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'С');     -- cyrilic
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'House');
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'house');
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'система');
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'Система');
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', 'ссубд');
select hasSubsequence(materialize('ClickHouse - столбцовая система управления базами данных'), 'субд');
select hasSubsequence(materialize('ClickHouse - столбцовая система управления базами данных'), 'суббд');
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', materialize('стул'));
select hasSubsequence('ClickHouse - столбцовая система управления базами данных', materialize('два стула'));
select hasSubsequence(materialize('ClickHouse - столбцовая система управления базами данных'), materialize('орех'));
select hasSubsequence(materialize('ClickHouse - столбцовая система управления базами данных'), materialize('два ореха'));

select 'hasSubsequenceCaseInsensitiveUTF8';
select hasSubsequenceCaseInsensitiveUTF8('для онлайн обработки аналитических запросов (OLAP)', 'oltp');
select hasSubsequenceCaseInsensitiveUTF8('для онлайн обработки аналитических запросов (OLAP)', 'оОоОоO');
select hasSubsequenceCaseInsensitiveUTF8('для онлайн обработки аналитических запросов (OLAP)', 'я раб');
select hasSubsequenceCaseInsensitiveUTF8(materialize('для онлайн обработки аналитических запросов (OLAP)'), 'работа');
select hasSubsequenceCaseInsensitiveUTF8(materialize('для онлайн обработки аналитических запросов (OLAP)'), 'work');
select hasSubsequenceCaseInsensitiveUTF8('для онлайн обработки аналитических запросов (OLAP)', materialize('добро)'));
select hasSubsequenceCaseInsensitiveUTF8('для онлайн обработки аналитических запросов (OLAP)', materialize('зло()'));
select hasSubsequenceCaseInsensitiveUTF8(materialize('для онлайн обработки аналитических запросов (OLAP)'), materialize('аналитика'));
select hasSubsequenceCaseInsensitiveUTF8(materialize('для онлайн обработки аналитических запросов (OLAP)'), materialize('аналитика для аналитиков'));

select 'Nullable';
select hasSubsequence(Null, Null);
select hasSubsequence(Null, 'a');
select hasSubsequence(Null::Nullable(String), 'arg'::Nullable(String));
select hasSubsequence('garbage'::Nullable(String), 'a');
select hasSubsequence('garbage'::Nullable(String), 'arg'::Nullable(String));
select hasSubsequence(materialize('garbage'::Nullable(String)), materialize('arg'::Nullable(String)));