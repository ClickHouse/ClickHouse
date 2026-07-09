---
description: 'TimeSeriesテーブルのデータを使ってPrometheusクエリを評価します。'
sidebar_label: 'prometheusQueryRange'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQueryRange
title: 'prometheusQueryRange'
doc_type: 'reference'
---

評価時刻の範囲にわたり、TimeSeriesテーブルのデータを使ってPrometheusクエリを評価します。

<div id="syntax">
  ## 構文
</div>

```sql
prometheusQueryRange('db_name', 'time_series_table', 'promql_query', start_time, end_time, step)
prometheusQueryRange(db_name.time_series_table, 'promql_query', start_time, end_time, step)
prometheusQueryRange('time_series_table', 'promql_query', start_time, end_time, step)
```

<div id="arguments">
  ## 引数
</div>

* `db_name` - TimeSeries テーブルが配置されているデータベースの名前。
* `time_series_table` - TimeSeries テーブルの名前。
* `promql_query` - [PromQL 構文](https://prometheus.io/docs/prometheus/latest/querying/basics/)で記述されたクエリ。
* `start_time` - 評価範囲の開始時刻。
* `end_time` - 評価範囲の終了時刻。
* `step` - `start_time` から `end_time` までの評価時刻を順にたどる際に使用する刻み幅 (両端を含む) 。

<div id="returned_value">
  ## 戻り値
</div>

この関数が返すカラムは、`promql_query` パラメータに渡すクエリの結果タイプによって異なります。

| 結果タイプ  | 結果カラム                                                                                     | 例                                                   |
| ------ | ----------------------------------------------------------------------------------------- | --------------------------------------------------- |
| vector | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType               | prometheusQuery(mytable, &#39;up&#39;)              |
| matrix | tags Array(Tuple(String, String)), time&#95;series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, &#39;up[1m]&#39;)          |
| scalar | scalar ValueType                                                                          | prometheusQuery(mytable, &#39;1h30m&#39;)           |
| string | string String                                                                             | prometheusQuery(mytable, &#39;&quot;abc&quot;&#39;) |

<div id="supported-promql-features">
  ## PromQLでサポートされている機能
</div>

<div id="selectors">
  ### セレクタ
</div>

インスタントセレクタ、レンジセレクタ、ラベルマッチャー (`=`、`!=`、`=~`、`!~`) 、offset修飾子、`@` タイムスタンプ修飾子、サブクエリ。

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

**注**: `histogram_quantile` は、クラシックヒストグラムのバケット (`le` ラベルで識別される) に対して線形補間を使用します。ネイティブヒストグラムはまだサポートされておらず、現在のところ `phi` (クォンタイルレベル) 引数には定数スカラーしか指定できません。`histogram_quantile(time() / 1000, ...)` のようにステップごとに値が変化する式は、`NOT_IMPLEMENTED` エラーで拒否されます。

<div id="operators">
  ### 演算子
</div>

算術 (`+`, `-`, `*`, `/`, `%`, `^`) 、比較 (`==`, `!=`, `<`, `>`, `<=`, `>=`。必要に応じて `bool` を指定可能) 、および論理 (`and`, `or`, `unless`) のすべての二項演算子に加え、`on()`/`ignoring()` と `group_left()`/`group_right()` 修飾子にも対応しています。

単項演算子 `+` と `-`。

<div id="aggregation-operators">
  ### 集計演算子
</div>

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — 必要に応じて `by()` または `without()` 修飾子を付けられます。

現時点では未対応: `count_values`。

<div id="example">
  ## 例
</div>

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```