---
toc_priority: 171
---

# timeSeriesGroupRateSum {#agg-function-timeseriesgroupratesum}

Синтаксис: `timeSeriesGroupRateSum(uid, ts, val)`

Аналогично timeSeriesGroupSum, timeSeriesGroupRateSum будет вычислять производные по timestamp для рядов, а затем суммировать полученные производные для всех рядов для одного значения timestamp.
Также ряды должны быть отсортированы по возрастанию timestamp.

Для пример из описания timeSeriesGroupSum результат будет следующим:

``` text
[(2,0),(3,0.1),(7,0.3),(8,0.3),(12,0.3),(17,0.3),(18,0.3),(24,0.3),(25,0.1)]
```

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/timeseriesgroupratesum/) <!--hide-->
