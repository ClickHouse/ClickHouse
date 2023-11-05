SELECT '1';
SELECT quantilesDDSketchArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, -inf), 1000000, inf));
SELECT quantilesDDSketchArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, inf), 1000000, -inf));
SELECT quantilesDDSketchArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, inf), 1000000, 0));
SELECT quantilesDDSketchArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([inf], 500000, -inf), 1000000, 0));
SELECT quantilesDDSketchArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([0], 500000, inf), 1000000, -inf));
SELECT quantilesDDSketchArray(0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99)(arrayResize(arrayResize([0], 500000, -inf), 1000000, inf));

SELECT '2';
SELECT quantilesDDSketch(0.01, 0.05)(x) FROM (SELECT inf*(number%2-0.5) x FROM numbers(300));
SELECT quantilesDDSketch(0.01, 0.5)(x) FROM (SELECT inf*(number%2-0.5) x FROM numbers(300));
SELECT quantilesDDSketch(0.01, 0.95)(x) FROM (SELECT inf*(number%2-0.5) x FROM numbers(300));

SELECT '3';
SELECT round(quantileDDSketch(0.01, 0.5)(number), 2) FROM numbers(200);
SELECT round(quantileDDSketch(0.01, 0.69)(number), 2) FROM numbers(500);
SELECT round(quantileDDSketch(0.01, 0.42)(number), 2) FROM numbers(200);
SELECT round(quantileDDSketch(0.01, 0.99)(number), 2) FROM numbers(500);
