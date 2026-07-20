-- Tags: no-object-storage, no-fasttest, no-cpu-aarch64, no-cpu-s390x
-- no-cpu-aarch64 and no-cpu-s390x because DEFLATE_QPL is x86-only
select * from system.codecs order by all;

select count() from system.codecs;

select name from system.columns where table = 'codecs' and database = 'system'
