-- Tags: no-object-storage, no-fasttest
select * from system.codecs order by name ASC COLLATE 'en';

select count() from system.codecs;

select name from system.columns where table = 'codecs' and database = 'system'
