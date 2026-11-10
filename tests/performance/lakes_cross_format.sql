SELECT c14, count(), avg(toUInt64OrZero(c2)) FROM upp_{format} GROUP BY c14 ORDER BY 2 DESC LIMIT 10;
SELECT count(), sum(toUInt64OrZero(c2)) FROM upp_{format} WHERE c12 = 'LONDON' SETTINGS delta_lake_enable_engine_predicate = 0;
SELECT c14, count(), avg(toUInt64OrZero(c2)) FROM upp_{format} GROUP BY c14 ORDER BY 2 DESC LIMIT 10;
SELECT count(), sum(toUInt64OrZero(c2)) FROM upp_{format} WHERE c12 = 'LONDON' SETTINGS delta_lake_enable_engine_predicate = 0;
SELECT c14, count(), avg(toUInt64OrZero(c2)) FROM upp_{format} GROUP BY c14 ORDER BY 2 DESC LIMIT 10;
SELECT count(), sum(toUInt64OrZero(c2)) FROM upp_{format} WHERE c12 = 'LONDON' SETTINGS delta_lake_enable_engine_predicate = 0;
