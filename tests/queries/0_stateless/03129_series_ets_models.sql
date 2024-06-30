SELECT('seriesHolt');

SELECT seriesHolt([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 3, 2, 4, 8, 12, 1], 6,  0.3, 0.4);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 5, 10, 13], 6, 0.5, 0.6);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 14, 17, 2,  3, 4], 6, 0.6, 0.2);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 0);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0, 0);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0, 1);
SELECT seriesHolt([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1);

SELECT('seriesAdditiveDamped');

SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 3, 2, 4, 8, 12, 1], 6,  0.3, 0.4, 0.2);
SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 5, 10, 13], 6, 0.5, 0.6, 0.4);
SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 14, 17, 2,  3, 4], 6, 0.6, 0.2, 0.8);
SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 0.2);
SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 1);
SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 0, 0);
SELECT seriesAdditiveDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1, 0.4);

SELECT('seriesMultiplicativeDamped');

SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 3, 2, 4, 8, 12, 1], 6,  0.3, 0.4, 0.2);
SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 5, 10, 13], 6, 0.5, 0.6, 0.4);
SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 14, 17, 2,  3, 4], 6, 0.6, 0.2, 0.8);
SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 0.2);
SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 1);
SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 0, 0);
SELECT seriesMultiplicativeDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1, 0.4);

SELECT('seriesHoltWintersAdditive');

SELECT seriesHoltWintersAdditive([4, 2, 5, 3, 1, 2, 8, 2, 6, 5, 4, 3, 2, 1, 14, 17, 2,  3, 42, 1, 3, 4, 6, 2], 6, 0.6, 0.2, 0.8, 3);
SELECT seriesHoltWintersAdditive([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 3, 2, 4, 8, 12, 1], 6,  0.3, 0.4, 0.2, 4);
SELECT seriesHoltWintersAdditive([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 5, 10, 13], 6, 0.5, 0.6, 0.4, 2);
SELECT seriesHoltWintersAdditive([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 0.2, 6);
SELECT seriesHoltWintersAdditive([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 1, 20);
SELECT seriesHoltWintersAdditive([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 0, 0, 3);
SELECT seriesHoltWintersAdditive([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1, 0.4, 2);

SELECT('seriesHoltWintersMultiplicative');

SELECT seriesHoltWintersMultiplicative([4, 2, 5, 3, 1, 2, 8, 2, 6, 5, 4, 3, 2, 1, 14, 17, 2,  3, 42, 1, 3, 4, 6, 2], 6, 0.6, 0.2, 0.8, 3);
SELECT seriesHoltWintersMultiplicative([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 3, 2, 4, 8, 12, 1], 6,  0.3, 0.4, 0.2, 4);
SELECT seriesHoltWintersMultiplicative([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 5, 10, 13], 6, 0.5, 0.6, 0.4, 2);
SELECT seriesHoltWintersMultiplicative([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 0.2, 6);
SELECT seriesHoltWintersMultiplicative([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 1, 20);
SELECT seriesHoltWintersMultiplicative([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 0, 0, 3);
SELECT seriesHoltWintersMultiplicative([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1, 0.4, 2);


SELECT('seriesHoltWintersDamped');

SELECT seriesHoltWintersDamped([1, 2], 6, 0.6, 0.2, 0.8, 0.2, 3);


SELECT seriesHoltWintersDamped([4, 2, 5, 3, 1, 2, 8, 2, 6, 5, 4, 3, 2, 1, 14, 17, 2,  3, 42, 1, 3, 4, 6, 2], 6, 0.6, 0.2, 0.8, 0.2, 3);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5, 3, 2, 4, 8, 12, 1], 6,  0.3, 0.4, 0.2, 0.4, 4);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 3, 5, 10, 13], 6, 0.5, 0.6, 0.4, 0.9, 2);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 0.2, 0.6, 6);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 1, 0.1, 20);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 0, 0, 1,  3);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1, 0.4, 0.8, 2);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 0.2, 0.8, 0.2, 0, 6);
SELECT seriesHoltWintersDamped([1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1, 5, 2, 4, 2, 6, 1], 6, 1, 1, 0.4, 1, 2);