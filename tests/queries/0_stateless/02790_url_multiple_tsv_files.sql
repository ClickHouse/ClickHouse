select sum(*) from (select * from url('http://127.0.0.1:8123?query=select+{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}+as+x+format+TSV', 'TSV') settings max_threads=1, max_download_threads=1);
select sum(*) from (select * from url('http://127.0.0.1:8123?query=select+{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}+as+x+format+CSV', 'CSV') settings max_threads=1, max_download_threads=1);
select sum(*) from (select * from url('http://127.0.0.1:8123?query=select+{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}+as+x+format+JSONEachRow', 'JSONEachRow') settings max_threads=1, max_download_threads=1);
select sum(*) from (select * from url('http://127.0.0.1:8123?query=select+{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}+as+x+format+TSKV', 'TSKV') settings max_threads=1, max_download_threads=1);
select sum(*) from (select * from url('http://127.0.0.1:8123?query=select+{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}+as+x+format+Native', 'Native') settings max_threads=1, max_download_threads=1);
