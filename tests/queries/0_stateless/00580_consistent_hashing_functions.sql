-- Tags: no-fasttest

SELECT jumpConsistentHash(1, 1), jumpConsistentHash(42, 57), jumpConsistentHash(256, 1024), jumpConsistentHash(3735883980, 1), jumpConsistentHash(3735883980, 666), jumpConsistentHash(16045690984833335023, 255);
SELECT kostikConsistentHash(16045690984833335023, 1), kostikConsistentHash(16045690984833335023, 2), kostikConsistentHash(16045690984833335023, 3), kostikConsistentHash(16045690984833335023, 4), kostikConsistentHash(16045690984833335023, 173), kostikConsistentHash(16045690984833335023, 255);
SELECT jumpConsistentHash(intHash64(number), 787) FROM system.numbers LIMIT 1000000, 2;
SELECT kostikConsistentHash(16045690984833335023+number-number, 120) FROM system.numbers LIMIT 1000000, 2;
