SELECT finalizeAggregation(10 * (
    SELECT medianState(number)
    FROM numbers(10)
));

SELECT finalizeAggregation(10 * (
    SELECT medianState(number)
    FROM numbers(1000)
));

SELECT finalizeAggregation(10 * (
    SELECT medianState(number)
    FROM numbers(100000)
));
