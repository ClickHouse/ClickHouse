set optimize_use_projections = 1, force_optimize_projection = 1;

drop table if exists tp;

create table tp (type Int32, device UUID, cnt UInt64) engine = MergeTree order by (type, device);
insert into tp select number%3, generateUUIDv4(), 1 from numbers(300);

alter table tp add projection uniq_city_proj ( select type, uniq(cityHash64(device)), sum(cnt) group by type );
alter table tp materialize projection uniq_city_proj settings mutations_sync = 1;

drop table tp;
