select  a, b || b from (select [number] as a, toString(number) as b from system.numbers limit 2);
