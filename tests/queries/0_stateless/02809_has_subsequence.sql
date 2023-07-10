select 'hasSubsequence / const / const';
select hasSubsequence('garbage', '');
select hasSubsequence('garbage', 'g');
select hasSubsequence('garbage', 'G');
select hasSubsequence('garbage', 'a');
select hasSubsequence('garbage', 'e');
select hasSubsequence('garbage', 'gr');
select hasSubsequence('garbage', 'ab');
select hasSubsequence('garbage', 'be');
select hasSubsequence('garbage', 'arg');
select hasSubsequence('garbage', 'garbage');

select hasSubsequence('garbage', 'garbage1');
select hasSubsequence('garbage', 'arbw');
select hasSubsequence('garbage', 'ARG');

select 'hasSubsequence / const / string';
select hasSubsequence('garbage', materialize(''));
select hasSubsequence('garbage', materialize('arg'));
select hasSubsequence('garbage', materialize('arbw'));

select 'hasSubsequence / string / const';
select hasSubsequence(materialize('garbage'), '');
select hasSubsequence(materialize('garbage'), 'arg');
select hasSubsequence(materialize('garbage'), 'arbw');

select 'hasSubsequence / string / string';

select hasSubsequence(materialize('garbage'), materialize(''));
select hasSubsequence(materialize('garbage'), materialize('arg'));
select hasSubsequence(materialize('garbage'), materialize('garbage1'));

select 'hasSubsequenceCaseInsensitive / const / const';

select hasSubsequenceCaseInsensitive('garbage', 'w');
select hasSubsequenceCaseInsensitive('garbage', 'ARG');
select hasSubsequenceCaseInsensitive('GARGAGE', 'arg');

select 'hasSubsequenceCaseInsensitive / string / string';
select hasSubsequenceCaseInsensitive(materialize('garbage'), materialize('w'));
select hasSubsequenceCaseInsensitive(materialize('garbage'), materialize('ARG'));
select hasSubsequenceCaseInsensitive(materialize('GARGAGE'), materialize('arg'));

select 'hasSubsequenceUTF8 / const / const';
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