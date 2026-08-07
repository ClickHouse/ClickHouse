SELECT finalizeAggregation((SELECT sumState(number) FROM numbers(10)) + (SELECT sumState(number) FROM numbers(10)));
SELECT finalizeAggregation((SELECT sumState(number) FROM numbers(10)) + materialize((SELECT sumState(number) FROM numbers(10))));
SELECT finalizeAggregation(materialize((SELECT sumState(number) FROM numbers(10))) + (SELECT sumState(number) FROM numbers(10)));
SELECT finalizeAggregation(materialize((SELECT sumState(number) FROM numbers(10))) + materialize((SELECT sumState(number) FROM numbers(10))));
SELECT finalizeAggregation(materialize((SELECT sumState(number) FROM numbers(10)) + (SELECT sumState(number) FROM numbers(10))));
SELECT materialize(finalizeAggregation((SELECT sumState(number) FROM numbers(10)) + (SELECT sumState(number) FROM numbers(10))));
