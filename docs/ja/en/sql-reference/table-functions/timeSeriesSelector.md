---
description: 'セレクタで絞り込み、指定した期間内のタイムスタンプを持つ TimeSeries テーブルから時系列を読み取ります。'
sidebar_label: 'timeSeriesSelector'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelector
title: 'timeSeriesSelector'
doc_type: 'reference'
---

セレクタで絞り込み、指定した期間内のタイムスタンプを持つ TimeSeries テーブルから時系列を読み取ります。
この関数は [range selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors) に似ていますが、[instant selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) の実装にも使用されます。

<div id="syntax">
  ## 構文
</div>

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

<div id="arguments">
  ## 引数
</div>

* `db_name` - TimeSeries テーブルが存在するデータベースの名前。
* `time_series_table` - TimeSeries テーブルの名前。
* `instant_query` - `@` または `offset` 修飾子を含まない、[PromQL 構文](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors)で記述されたインスタントセレクタ。
* &#96;min&#95;time - 開始タイムスタンプ (この値を含む) 。
* &#96;max&#95;time - 終了タイムスタンプ (この値を含む) 。

<div id="returned_value">
  ## 戻り値
</div>

この関数は 3 つのカラムを返します。

* `id` - 指定したセレクタに一致する時系列の識別子が含まれます。
* `timestamp` - タイムスタンプが含まれます。
* `value` - 値が含まれます。

返されるデータの順序は特定されていません。

<div id="example">
  ## 例
</div>

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```