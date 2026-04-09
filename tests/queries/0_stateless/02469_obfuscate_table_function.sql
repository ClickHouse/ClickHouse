-- TODO: Currently seed is fixed inside obfuscate function, we need to add it as a setting

SET enable_analyzer = 0;

select * from obfuscate(select * from numbers(5)) limit 10;
