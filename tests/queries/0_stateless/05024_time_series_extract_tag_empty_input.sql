SELECT 'timeSeriesExtractTag empty input:';

SELECT timeSeriesExtractTag(number, 'env')
FROM numbers(0);
