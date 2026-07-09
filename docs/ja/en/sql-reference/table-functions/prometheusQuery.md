---
description: 'TimeSeries テーブルのデータを使用して prometheus クエリを評価します。'
sidebar_label: 'prometheusQuery'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQuery
title: 'prometheusQuery'
doc_type: 'reference'
---

TimeSeries テーブルのデータを使用して prometheus クエリを評価します。

<div id="syntax">
  ## 構文
</div>

```sql
prometheusQuery('db_name', 'time_series_table', 'promql_query', evaluation_time)
prometheusQuery(db_name.time_series_table, 'promql_query', evaluation_time)
prometheusQuery('time_series_table', 'promql_query', evaluation_time)
```

<div id="arguments">
  ## 引数
</div>

* `db_name` - TimeSeries テーブルが配置されているデータベースの名前。
* `time_series_table` - TimeSeries テーブルの名前。
* `promql_query` - [PromQL 構文](https://prometheus.io/docs/prometheus/latest/querying/basics/)で記述されたクエリ。
* `evaluation_time - 評価時刻の timestamp。現在時刻でクエリを評価するには、`evaluation&#95;time`として`now()&#96; を使用します。

<div id="returned_value">
  ## 戻り値
</div>

この関数は、パラメータ `promql_query` に渡されたクエリの結果タイプに応じて、異なるカラムを返します。

| 結果タイプ  | 結果カラム                                                                                     | 例                                                   |
| ------ | ----------------------------------------------------------------------------------------- | --------------------------------------------------- |
| vector | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType               | prometheusQuery(mytable, &#39;up&#39;)              |
| matrix | tags Array(Tuple(String, String)), time&#95;series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, &#39;up[1m]&#39;)          |
| scalar | scalar ValueType                                                                          | prometheusQuery(mytable, &#39;1h30m&#39;)           |
| string | string String                                                                             | prometheusQuery(mytable, &#39;&quot;abc&quot;&#39;) |

<div id="supported-promql-features">
  ## 対応している PromQL 機能
</div>

<div id="selectors">
  ### セレクタ
</div>

インスタントセレクタ、レンジセレクタ、ラベルマッチャー (`=`, `!=`, `=~`, `!~`) 、offset修飾子、`@` タイムスタンプ修飾子、サブクエリ。

<div id="functions">
  ### 関数
</div>

| カテゴリ     | 関数                                                                                               |
| -------- | ------------------------------------------------------------------------------------------------ |
| 範囲       | `rate`, `irate`, `delta`, `idelta`, `last_over_time`                                             |
| 数学       | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`                |
| 三角関数     | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh`   |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| 型        | `scalar`, `vector`                                                                               |
| ヒストグラム   | `histogram_quantile`                                                                             |
| その他      | `time`, `pi`                                                                                     |

**注**: `histogram_quantile` は、従来のヒストグラムバケット (`le` ラベルで識別) に対して線形補間を使用します。ネイティブヒストグラムはまだサポートされておらず、また `phi` (quantile level) 引数は現時点では定数のスカラーである必要があります。`histogram_quantile(time() / 1000, ...)` のようにステップごとに変化する式は、`NOT_IMPLEMENTED` エラーで拒否されます。

<div id="operators">
  ### 演算子
</div>

`on()`/`ignoring()` および `group_left()`/`group_right()` 修飾子を伴う、すべての算術 (`+`、`-`、`*`、`/`、`%`、`^`) 、比較 (`==`、`!=`、`<`、`>`、`<=`、`>=`。オプションで `bool` を指定可能) 、および論理 (`and`、`or`、`unless`) の二項演算子。

単項演算子 `+` および `-`。

<div id="aggregation-operators">
  ### 集約演算子
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — オプションで `by()` または `without()` 修飾子を指定できます。

`count_values` は現在サポートされていません。

<div id="example">
  ## 例
</div>

```sql
SELECT * FROM prometheusQuery(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now())
```