create table test (a Tuple(b String, c Tuple(d Nullable(UInt64), e Array(UInt32), f Array(Tuple(g String, h Map(String, Array(Tuple(i String, j UInt64))))), k Date), l Nullable(String))) engine=Memory;
insert into test select * from generateRandom(42) limit 1;
set print_pretty_type_names=1;
desc test format TSVRaw;
select toTypeName(a) from test limit 1 format TSVRaw;
