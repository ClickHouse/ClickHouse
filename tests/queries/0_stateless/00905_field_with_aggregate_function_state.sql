with (select sumState(1)) as s select sumMerge(s);
with (select sumState(number) from (select * from system.numbers limit 10)) as s select sumMerge(s);
with (select quantileState(0.5)(number) from (select * from system.numbers limit 10)) as s select quantileMerge(s);

