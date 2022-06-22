select * from url(url_with_headers, url='http://127.0.0.1:8123?query=select+12', format='RawBLOB');
select * from url('http://127.0.0.1:8123?query=select+12', 'RawBLOB', headers('X-ClickHouse-Format'='JSONEachRow'));
select * from url('http://127.0.0.1:8123?query=select+12', 'RawBLOB', headers('X-ClickHouse-Format'='JSONEachRow', 'X-ClickHouse-Database'='kek')); -- { serverError 86 }
select * from url('http://127.0.0.1:8123?query=select+12', 'RawBLOB', headers('X-ClickHouse-Format'='JSONEachRow', 'X-ClickHouse-Database'=1)); -- { serverError 36 }
drop table if exists url;
create table url (i String) engine=URL('http://127.0.0.1:8123?query=select+12', 'RawBLOB', headers('X-ClickHouse-Format'='JSONEachRow'));
select * from url;
