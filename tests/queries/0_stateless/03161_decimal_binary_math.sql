SELECT toDecimal32('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(pow(x, y), 6);
SELECT toDecimal64('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(pow(x, y), 6);
SELECT toDecimal32('42.4242', 4) AS x, toDecimal64('2.42', 2) AS y, round(pow(x, y), 6);
SELECT toDecimal64('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(pow(x, y), 6);
SELECT toDecimal32('42.4242', 4) AS x, materialize(toDecimal32('2.42', 2)) AS y, round(pow(x, y), 6);
SELECT materialize(toDecimal32('42.4242', 4)) AS x, toDecimal32('2.42', 2) AS y, round(pow(x, y), 6);
SELECT materialize(toDecimal32('42.4242', 4)) AS x, materialize(toDecimal32('2.42', 2)) AS y, round(pow(x, y), 6);

SELECT toDecimal32('0.4242', 4) AS x, toDecimal32('0.24', 2) AS y, round(atan2(y, x), 6);
SELECT toDecimal64('0.4242', 4) AS x, toDecimal32('0.24', 2) AS y, round(atan2(y, x), 6);
SELECT toDecimal32('0.4242', 4) AS x, toDecimal64('0.24', 2) AS y, round(atan2(y, x), 6);
SELECT toDecimal64('0.4242', 4) AS x, toDecimal64('0.24', 2) AS y, round(atan2(y, x), 6);
SELECT toDecimal32('0.4242', 4) AS x, materialize(toDecimal32('0.24', 2)) AS y, round(atan2(y, x), 6);
SELECT materialize(toDecimal32('0.4242', 4)) AS x, toDecimal32('0.24', 2) AS y, round(atan2(y, x), 6);
SELECT materialize(toDecimal32('0.4242', 4)) AS x, materialize(toDecimal32('0.24', 2)) AS y, round(atan2(y, x), 6);

SELECT toDecimal32('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(min2(x, y), 6);
SELECT toDecimal64('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(min2(x, y), 6);
SELECT toDecimal32('42.4242', 4) AS x, toDecimal64('2.42', 2) AS y, round(min2(x, y), 6);
SELECT toDecimal64('42.4242', 4) AS x, toDecimal64('2.42', 2) AS y, round(min2(x, y), 6);
SELECT toDecimal32('42.4242', 4) AS x, materialize(toDecimal32('2.42', 2)) AS y, round(min2(x, y), 6);
SELECT materialize(toDecimal32('42.4242', 4)) AS x, toDecimal32('2.42', 2) AS y, round(min2(x, y), 6);
SELECT materialize(toDecimal32('42.4242', 4)) AS x, materialize(toDecimal32('2.42', 2)) AS y, round(min2(x, y), 6);

SELECT toDecimal32('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(max2(x, y), 6);
SELECT toDecimal64('42.4242', 4) AS x, toDecimal32('2.42', 2) AS y, round(max2(x, y), 6);
SELECT toDecimal32('42.4242', 4) AS x, toDecimal64('2.42', 2) AS y, round(max2(x, y), 6);
SELECT toDecimal64('42.4242', 4) AS x, toDecimal64('2.42', 2) AS y, round(max2(x, y), 6);
SELECT toDecimal32('42.4242', 4) AS x, materialize(toDecimal32('2.42', 2)) AS y, round(max2(x, y), 6);
SELECT materialize(toDecimal32('42.4242', 4)) AS x, toDecimal32('2.42', 2) AS y, round(max2(x, y), 6);
SELECT materialize(toDecimal32('42.4242', 4)) AS x, materialize(toDecimal32('2.42', 2)) AS y, round(max2(x, y), 6);

SELECT toDecimal32('0.4242', 4) AS x, toDecimal32('0.4242', 4) AS y, round(hypot(x, y), 6);
SELECT toDecimal64('0.4242', 4) AS x, toDecimal32('0.4242', 4) AS y, round(hypot(x, y), 6);
SELECT toDecimal32('0.4242', 4) AS x, toDecimal64('0.4242', 4) AS y, round(hypot(x, y), 6);
SELECT toDecimal64('0.4242', 4) AS x, toDecimal64('0.4242', 4) AS y, round(hypot(x, y), 6);
SELECT toDecimal32('0.4242', 4) AS x, materialize(toDecimal32('0.4242', 4)) AS y, round(hypot(x, y), 6);
SELECT materialize(toDecimal32('0.4242', 4)) AS x, toDecimal32('0.4242', 4) AS y, round(hypot(x, y), 6);
SELECT materialize(toDecimal32('0.4242', 4)) AS x, materialize(toDecimal32('0.4242', 4)) AS y, round(hypot(x, y), 6);
