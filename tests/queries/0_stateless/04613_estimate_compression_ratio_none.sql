-- Should match.
SELECT round(estimateCompressionRatio('NONE', 65536)(number), 6) FROM numbers(100000);
SELECT round(800000 / (800000 + 13 * 25), 6);
