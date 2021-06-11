---
toc_priority: 171
---

# timeSeriesGroupRateSum {#agg-function-timeseriesgroupratesum}

Syntax: `timeSeriesGroupRateSum(uid, ts, val)`

Similarly to [timeSeriesGroupSum](../../../sql-reference/aggregate-functions/reference/timeseriesgroupsum.md), `timeSeriesGroupRateSum` calculates the rate of time-series and then sum rates together.
Also, timestamp should be in ascend order before use this function.

Applying this function to the data from the `timeSeriesGroupSum` example, you get the following result:

``` text
[(2,0),(3,0.1),(7,0.3),(8,0.3),(12,0.3),(17,0.3),(18,0.3),(24,0.3),(25,0.1)]
```
