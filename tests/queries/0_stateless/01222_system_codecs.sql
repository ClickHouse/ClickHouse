-- Tags: no-object-storage
select * from system.codecs where codec = 'LZ4';
select count() from system.codecs;

select name from system.columns where table = 'codecs' and database = 'system'
