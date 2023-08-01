select toString(x)::Decimal(6, 3) from generateRandom('x Decimal(6, 3)', 42) limit 5;
select toString(x)::Decimal(15, 9) from generateRandom('x Decimal(15, 9)', 42) limit 5;
select toString(x)::Decimal(30, 20) from generateRandom('x Decimal(30, 20)', 42) limit 5;
select toString(x)::Decimal(60, 40) from generateRandom('x Decimal(60, 40)', 42) limit 5;
select reinterpret(x, 'UInt8') from generateRandom('x Bool', 42) limit 5;
