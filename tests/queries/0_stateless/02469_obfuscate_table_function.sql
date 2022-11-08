-- TODO: Currently seed is fixed inside obfuscate function, we need to add it as a setting

select * from obfuscate(select * from numbers(5)) limit 10;
