-- { echo }
SELECT avgWeighted(number, number) t, toTypeName(t) FROM numbers(1);
SELECT avgWeighted(number, number + 1) t, toTypeName(t) FROM numbers(0);

SELECT avgWeighted(toNullable(number), number) t, toTypeName(t) FROM numbers(1);
SELECT avgWeighted(if(number < 10000, NULL, number), number) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(if(number < 50, NULL, number), number) t, toTypeName(t) FROM numbers(100);

SELECT avgWeighted(number, if(number < 10000, NULL, number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(number, if(number < 50, NULL, number)) t, toTypeName(t) FROM numbers(100);

SELECT avgWeighted(toNullable(number), if(number < 10000, NULL, number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(toNullable(number), if(number < 50, NULL, number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(if(number < 10000, NULL, number), toNullable(number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(if(number < 50, NULL, number), toNullable(number)) t, toTypeName(t) FROM numbers(100);

SELECT avgWeighted(toNullable(number), if(number < 500, NULL, number)) t, toTypeName(t) FROM numbers(1000);

SELECT avgWeighted(if(number < 10000, NULL, number), if(number < 10000, NULL, number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(if(number < 50, NULL, number), if(number < 10000, NULL, number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(if(number < 10000, NULL, number), if(number < 50, NULL, number)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeighted(if(number < 50, NULL, number), if(number < 50, NULL, number)) t, toTypeName(t) FROM numbers(100);

SELECT avgWeighted(if(number < 10000, NULL, number), if(number < 500, NULL, number)) t, toTypeName(t) FROM numbers(1000);

SELECT avgWeightedIf(number, number, number % 10) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(number, number, toNullable(number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(number, number, if(number < 10000, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(number, number, if(number < 50, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(number, number, if(number < 0, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);

SELECT avgWeightedIf(number, number, toNullable(number % 10)) t, toTypeName(t) FROM numbers(1000);

SELECT avgWeightedIf(if(number < 10000, NULL, number), if(number < 10000, NULL, number), if(number < 10000, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 50, NULL, number), if(number < 10000, NULL, number), if(number < 10000, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 10000, NULL, number), if(number < 50, NULL, number), if(number < 10000, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 50, NULL, number), if(number < 50, NULL, number), if(number < 10000, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);

SELECT avgWeightedIf(if(number < 10000, NULL, number), if(number < 10000, NULL, number), if(number < 50, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 50, NULL, number), if(number < 10000, NULL, number), if(number < 50, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 10000, NULL, number), if(number < 50, NULL, number), if(number < 50, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 50, NULL, number), if(number < 50, NULL, number), if(number < 50, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);

SELECT avgWeightedIf(if(number < 10000, NULL, number), if(number < 10000, NULL, number), if(number < 0, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 50, NULL, number), if(number < 10000, NULL, number), if(number < 0, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 10000, NULL, number), if(number < 50, NULL, number), if(number < 0, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
SELECT avgWeightedIf(if(number < 50, NULL, number), if(number < 50, NULL, number), if(number < 0, NULL, number % 10)) t, toTypeName(t) FROM numbers(100);
