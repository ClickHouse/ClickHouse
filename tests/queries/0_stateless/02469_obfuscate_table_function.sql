-- Pin the seed so the output is deterministic. Without a seed the obfuscate
-- table function picks a random one per query.

select * from obfuscate(select * from numbers(5)) limit 10 SETTINGS obfuscate_seed = 'default-test-seed';
