-- Tags: no-object-storage, no-fasttest
select * from system.codecs;

select count() from system.codecs;

select name from system.columns where table = 'codecs' and database = 'system'
