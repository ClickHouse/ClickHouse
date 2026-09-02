SELECT [number][10000000000] FROM numbers(1);
SELECT [number][-10000000000] FROM numbers(1);

SELECT [number][-0x8000000000000000] FROM numbers(1);
SELECT [number][0xFFFFFFFFFFFFFFFF] FROM numbers(1);

SELECT '---';

SELECT [materialize(1)][0xFFFFFFFFFFFFFFFF];
SELECT [materialize(1)][materialize(18446744073709551615)];
SELECT [materialize(1)][-0x8000000000000000];

SELECT '---';

SELECT [number][arrayJoin([-0x8000000000000000, -10000000000, 0, -1])] FROM numbers(2);

SELECT '---';

SELECT [number][arrayJoin([0xFFFFFFFFFFFFFFFF, 10000000000, 0, 1])] FROM numbers(2);
