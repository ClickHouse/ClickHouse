SELECT varSamp(0.1) FROM numbers(1000000);
SELECT varPop(0.1) FROM numbers(1000000);
SELECT stddevSamp(0.1) FROM numbers(1000000);
SELECT stddevPop(0.1) FROM numbers(1000000);

SELECT varSampStable(0.1) FROM numbers(1000000);
SELECT varPopStable(0.1) FROM numbers(1000000);
SELECT stddevSampStable(0.1) FROM numbers(1000000);
SELECT stddevPopStable(0.1) FROM numbers(1000000);
