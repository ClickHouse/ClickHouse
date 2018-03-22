select quantilesDeterministic(0.5, 0.9)(number, number) from (select number from system.numbers limit 101);
-- test merge does not cause overflow
select ignore(quantilesDeterministic(0.5, 0.9)(number, number)) from (select number from remote('127.0.0.{2,3}', system, numbers) limit 1000000);
