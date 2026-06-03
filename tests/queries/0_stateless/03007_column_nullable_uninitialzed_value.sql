SELECT count(NULL) IGNORE NULLS > avg(toDecimal32(NULL)) IGNORE NULLS, count() FROM numbers(1000) WITH TOTALS SETTINGS enable_analyzer = 1;
