SELECT '1';
SELECT round(quantileDDSketch(0.01, 0.5)(number), 2) FROM numbers(200);
SELECT round(quantileDDSketch(0.01, 0.69)(number), 2) FROM numbers(500);
SELECT round(quantileDDSketch(0.01, 0.42)(number), 2) FROM numbers(200);
SELECT round(quantileDDSketch(0.01, 0.99)(number), 2) FROM numbers(500);
