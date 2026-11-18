SELECT 'timeSeriesExtractTag empty input:';

SELECT timeSeriesExtractTag([('env', toString(number))], 'env')
FROM numbers(0);
