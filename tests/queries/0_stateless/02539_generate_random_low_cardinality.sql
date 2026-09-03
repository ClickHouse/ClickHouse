-- Check that the function works for LowCardinality and gives at least something plausible:
SELECT uniq(x) > 1000 FROM (SELECT * FROM generateRandom('x Array(LowCardinality(Nullable(String)))') LIMIT 100000);
