---
description: 'timeSeriesMetrics는 테이블 엔진이 TimeSeries인 `db_name.time_series_table` 테이블에서
  사용하는 Metrics 테이블을 반환합니다.'
sidebar_label: 'timeSeriesMetrics'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesMetrics
title: 'timeSeriesMetrics'
doc_type: '참고'
---

`timeSeriesMetrics(db_name.time_series_table)` - 테이블 엔진이 [TimeSeries](../../engines/table-engines/integrations/time-series.md)인 `db_name.time_series_table` 테이블에서 사용하는 [Metrics 테이블](../../engines/table-engines/integrations/time-series.md#metrics-table)을
반환합니다:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

함수는 *metrics* 테이블이 내부 테이블이어도 작동합니다.

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

다음 쿼리는 서로 같습니다:

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```