SELECT hex(toString(uniqStateForEach([1, NULL])));
SELECT hex(toString(uniqStateForEachState([1, NULL])));
SELECT arrayMap(x -> hex(toString(x)), finalizeAggregation(uniqStateForEachState([1, NULL])));
SELECT arrayMap(x -> finalizeAggregation(x), finalizeAggregation(uniqStateForEachState([1, NULL])));

SELECT hex(toString(uniqStateForEach([1, NULL]))) WITH TOTALS;
SELECT hex(toString(uniqStateForEachState([1, NULL]))) WITH TOTALS;
SELECT arrayMap(x -> hex(toString(x)), finalizeAggregation(uniqStateForEachState([1, NULL]))) WITH TOTALS;
SELECT arrayMap(x -> finalizeAggregation(x), finalizeAggregation(uniqStateForEachState([1, NULL]))) WITH TOTALS;
