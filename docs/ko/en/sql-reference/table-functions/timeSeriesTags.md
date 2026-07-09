---
description: 'timeSeriesTags 테이블 함수는 테이블 엔진이 TimeSeries 엔진인 테이블 `db_name.time_series_table`에서
  사용하는 tags 테이블을 반환합니다.'
sidebar_label: 'timeSeriesTags'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesTags
title: 'timeSeriesTags'
doc_type: 'reference'
---

`timeSeriesTags(db_name.time_series_table)` - 테이블 엔진이 [TimeSeries](../../engines/table-engines/integrations/time-series.md) 엔진인 테이블 `db_name.time_series_table`에서
사용하는 [tags](../../engines/table-engines/integrations/time-series.md#tags-table) 테이블을 반환합니다:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

*tags* 테이블이 inner인 경우에도 이 함수는 동작합니다:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

다음 쿼리는 서로 동일합니다:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```