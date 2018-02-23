SELECT JumpConsistentHash(1, 1), JumpConsistentHash(42, 57), JumpConsistentHash(256, 1024), JumpConsistentHash(3735883980, 1), JumpConsistentHash(3735883980, 666), JumpConsistentHash(16045690984833335023, 255);
SELECT YandexConsistentHash(16045690984833335023, 1), YandexConsistentHash(16045690984833335023, 2), YandexConsistentHash(16045690984833335023, 3), YandexConsistentHash(16045690984833335023, 4), YandexConsistentHash(16045690984833335023, 173), YandexConsistentHash(16045690984833335023, 255);
SELECT JumpConsistentHash(intHash64(number), 787) FROM system.numbers LIMIT 1000000, 2;
SELECT YandexConsistentHash(16045690984833335023+number-number, 120) FROM system.numbers LIMIT 1000000, 2;
