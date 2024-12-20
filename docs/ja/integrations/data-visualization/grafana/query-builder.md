---
sidebar_label: クエリビルダー
sidebar_position: 2
slug: /ja/integrations/grafana/query-builder
description: ClickHouse Grafana プラグインでのクエリビルダーの使用
---

# クエリビルダー

任意のクエリは ClickHouse プラグインで実行できます。
クエリビルダーはシンプルなクエリには便利なオプションですが、複雑なクエリには [SQL エディタ](#sql-editor)を使用する必要があります。

クエリビルダー内のすべてのクエリには[クエリタイプ](#query-types)があり、少なくとも1つのカラムを選択する必要があります。

使用可能なクエリタイプは次のとおりです：
- [Table](#table): データを表形式で表示するための最も簡単なクエリタイプ。集計関数を含む単純なクエリにも複雑なクエリにも適しています。
- [Logs](#logs): ログのクエリ作成に最適化されています。[デフォルト設定](./config.md#logs)されたエクスプローラビューで最適に機能します。
- [Time Series](#time-series): 時系列クエリの作成に最適です。専用の時間カラムを選択し、集計関数を追加できます。
- [Traces](#traces): トレースの検索/表示に最適化されています。[デフォルト設定](./config.md#traces)されたエクスプローラビューで最適に機能します。
- [SQL Editor](#sql-editor): クエリの完全な制御を行いたい場合に使用できます。このモードでは任意の SQL クエリを実行できます。

## クエリタイプ

*クエリタイプ*設定は、作成中のクエリのタイプに合うようにクエリビルダーのレイアウトを変更します。
クエリタイプは、データの可視化に使用されるパネルも決定します。

### Table

最も柔軟なクエリタイプは Table クエリです。これは、単純および集計クエリを処理するために設計された他のクエリビルダーを包括します。

| フィールド | 説明 |
|----|----|
| ビルダーモード  | 単純なクエリは集計や Group By を含まず、集計クエリはこれらのオプションを含みます。 |
| カラム | 選択されたカラム。このフィールドに生の SQL を入力して、関数やカラムのエイリアシングを許可します。 |
| 集計 | [集計関数](/docs/ja/sql-reference/aggregate-functions/index.md)のリスト。関数とカラムのカスタム値を許可します。集計モードでのみ表示されます。 |
| Group By | [GROUP BY](/docs/ja/sql-reference/statements/select/group-by.md) 式のリスト。集計モードでのみ表示されます。 |
| Order By | [ORDER BY](/docs/ja/sql-reference/statements/select/order-by.md) 式のリスト。 |
| Limit | クエリの末尾に [LIMIT](/docs/ja/sql-reference/statements/select/limit.md) 文を追加します。`0` に設定すると除外されます。一部の可視化では、すべてのデータを表示するために `0` に設定する必要があります。 |
| フィルター | `WHERE` 句に適用されるフィルターのリスト。 |

<img src={require('./images/demo_table_query.png').default} class="image" alt="Example aggregate table query" />

このクエリタイプは、データをテーブルとしてレンダリングします。

### Logs

Logs クエリタイプは、ログデータのクエリ作成に焦点を当てたクエリビルダーを提供します。
デフォルトはデータソースの[ログ設定](./config.md#logs)で設定することができ、クエリビルダーをデフォルトのデータベース/テーブルとカラムでプレロードできます。
また、OpenTelemetry を有効にして、スキーマバージョンに応じたカラムを自動選択することもできます。

**時間**と**レベル**フィルターがデフォルトで追加され、時間カラムの Order By も含まれます。
これらのフィルターは各フィールドに関連付けられており、カラムが変更されると更新されます。
**レベル**フィルターはデフォルトでは SQL から除外されており、`IS ANYTHING` オプションから変更することで有効になります。

Logs クエリタイプは[データリンク](#data-links)をサポートしています。

| フィールド | 説明 |
|----|----|
| Use OTel | OpenTelemetry カラムを有効にします。選択された OTel スキーマバージョンで定義されたカラムを使用するように選択されたカラムを上書きします（カラム選択を無効にします）。 |
| カラム | ログ行に追加されるカラム。生の SQL をこのフィールドに入力して関数やカラムのエイリアシングを許可します。 |
| 時間 | ログのプライマリタイムスタンプカラム。時間的な型を表示しますが、カスタム値/関数も許可されます。 |
| ログレベル | 任意。ログの*レベル*または*重大度*。通常は`INFO`, `error`, `Debug`などの値が使用されます。 |
| メッセージ | ログメッセージの内容。 |
| Order By | [ORDER BY](/docs/ja/sql-reference/statements/select/order-by.md) 式のリスト。 |
| Limit | クエリの末尾に [LIMIT](/docs/ja/sql-reference/statements/select/limit.md) 文を追加します。`0` に設定すると除外されますが、大規模なログデータセットには推奨されません。 |
| フィルター | `WHERE` 句に適用されるフィルターのリスト。 |
| メッセージフィルター | `LIKE %value%` を使用してログを便利にフィルターリングするためのテキスト入力。入力が空の場合は除外されます。 |

<img src={require('./images/demo_logs_query.png').default} class="image" alt="Example OTel logs query" />

<br/>
このクエリタイプは、上部にログヒストグラムパネルとともに、ログパネルとしてデータをレンダリングします。

クエリ内で選択された追加カラムは、展開されたログ行内で確認できます：
<img src={require('./images/demo_logs_query_fields.png').default} class="image" alt="Example of extra fields on logs query" />

### Time Series

Time Series クエリタイプは[table](#table)に似ていますが、時系列データに焦点を当てています。

2つのビューはほぼ同じですが、以下の重要な違いがあります：
- 専用の*時間*フィールド。
- 集計モードでは、時間間隔マクロが自動的に適用され、Time フィールドに対する Group By が追加されます。
- 集計モードでは、「カラム」フィールドが非表示になります。
- 時間範囲フィルターと Order By が **Time** フィールドに自動的に追加されます。

:::重要 あなたのビジュアライゼーションがデータを欠いている場合
場合によっては、時系列パネルが `LIMIT` がデフォルトで `1000` に設定されているために切り捨てられたように表示されることがあります。

データセットが許せば、`LIMIT` 条項を削除して `0` に設定してみてください。
:::

| フィールド | 説明 |
|----|----|
| ビルダーモード  | 単純なクエリは集計および Group By を含まず、集計クエリはこれらのオプションを含みます。  |
| 時間 | クエリのプライマリ時間カラム。時間的な型を表示しますが、カスタム値/関数も許可されます。 |
| カラム | 選択されたカラム。このフィールドに生の SQL を入力して、関数やカラムのエイリアシングを許可します。単純モードでのみ表示されます。 |
| 集計 | [集計関数](/docs/ja/sql-reference/aggregate-functions/index.md)のリスト。関数とカラムのカスタム値を許可します。集計モードでのみ表示されます。 |
| Group By | [GROUP BY](/docs/ja/sql-reference/statements/select/group-by.md) 式のリスト。集計モードでのみ表示されます。 |
| Order By | [ORDER BY](/docs/ja/sql-reference/statements/select/order-by.md) 式のリスト。 |
| Limit | クエリの末尾に [LIMIT](/docs/ja/sql-reference/statements/select/limit.md) 文を追加します。`0` に設定すると除外されますが、時系列データセットではフルビジュアライゼーションを表示するために推奨されます。 |
| フィルター | `WHERE` 句に適用されるフィルターのリスト。 |

<img src={require('./images/demo_time_series_query.png').default} class="image" alt="Example time series query" />

このクエリタイプは、時系列パネルでデータをレンダリングします。

### Traces

Traces クエリタイプは、トレースを簡単に検索および表示するためのクエリビルダーを提供します。
これは OpenTelemetry データ用に設計されていますが、別のスキーマからトレースをレンダリングするためにカラムを選択することもできます。
デフォルトはデータソースの[トレース設定](./config.md#traces)で設定することができ、クエリビルダーをデフォルトのデータベース/テーブルとカラムでプレロードできます。デフォルトが設定されている場合、カラム選択はデフォルトで折りたたまれます。
また、OpenTelemetry を有効にして、スキーマバージョンに応じたカラムを自動選択することもできます。

デフォルトのフィルターは、トップレベルのスパンのみを表示する意図で追加されています。
Time および Duration Time の Order By も含まれています。
これらのフィルターは各フィールドに関連付けられており、カラムが変更されると更新されます。
**サービス名**フィルターはデフォルトでは SQL から除外されており、`IS ANYTHING` オプションから変更することで有効になります。

Traces クエリタイプは[データリンク](#data-links)をサポートしています。

| フィールド | 説明 |
|----|----|
| Trace モード | クエリを Trace Search から Trace ID 検索に変更します。 |
| Use OTel | OpenTelemetry カラムを有効にします。選択された OTel スキーマバージョンで定義されたカラムを使用するように選択されたカラムを上書きします（カラム選択を無効にします）。 |
| Trace ID カラム | トレースの ID。 |
| Span ID カラム | スパン ID。 |
| Parent Span ID カラム | 親スパン ID。これは通常、トップレベルトレースでは空です。 |
| サービス名カラム | サービス名。 |
| 操作名カラム | 操作名。 |
| 開始時間カラム | トレーススパンのプライマリ時間カラム。スパンが開始された時刻。 |
| 期間時間カラム | スパンの期間。Grafana はデフォルトでこれをミリ秒の浮動小数点数で期待しています。変換は`Duration Unit`ドロップダウンを介して自動的に適用されます。 |
| 期間単位 | 期間に使用される時間の単位。デフォルトではナノ秒。選択された単位は、Grafana に必要なミリ秒の浮動小数点数に変換されます。 |
| タグカラム | スパンタグ。OTel に基づくスキーマを使用していない場合は除外します。特定のマップカラムタイプを期待します。 |
| サービスタグカラム | サービスタグ。OTel に基づくスキーマを使用していない場合は除外します。特定のマップカラムタイプを期待します。 |
| Order By | [ORDER BY](/docs/ja/sql-reference/statements/select/order-by.md) 式のリスト。 |
| Limit | クエリの末尾に [LIMIT](/docs/ja/sql-reference/statements/select/limit.md) 文を追加します。`0` に設定すると除外されますが、大規模なトレースデータセットには推奨されません。 |
| フィルター | `WHERE` 句に適用されるフィルターのリスト。 |
| Trace ID | フィルターで使用するトレース ID。Trace ID モードでのみ使用され、Trace ID [データリンク](#data-links)を開くときに使用されます。 |

<img src={require('./images/demo_trace_query.png').default} class="image" alt="Example OTel trace query" />

このクエリタイプは、Trace Search モードではテーブルビューで、Trace ID モードではトレースパネルでデータをレンダリングします。

## SQL エディタ

クエリビルダーには複雑すぎるクエリの場合は、SQL エディタを使用できます。
これにより、生の ClickHouse SQL を記述して実行することでクエリを完全に制御できます。

SQL エディタは、クエリエディタの上部で「SQL Editor」を選択することで開くことができます。

このモードでも[マクロ関数](#macros)を使用できます。

クエリタイプを切り替えて、クエリに最も適したビジュアライゼーションを取得できます。
ダッシュボードビュー、特に時系列データでは、この切り替えも効果があります。

<img src={require('./images/demo_raw_sql_query.png').default} class="image" alt="Example raw SQL query" />

## データリンク

Grafana [データリンク](https://grafana.com/docs/grafana/latest/panels-visualizations/configure-data-links)
は新しいクエリへのリンクを作成するために使用できます。
この機能は、ClickHouse プラグイン内でトレースをログにリンクしたりその逆を実行したりするために有効化されています。
データソースの[設定](./config.md#opentelemetry)で OpenTelemetry を両方のログとトレースに設定すると最適に機能します。

<div style={{display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'space-between', marginBottom: '15px' }}>
  テーブル内のトレースリンクの例
  <img src={require('./images/trace_id_in_table.png').default} class="image" alt="Trace links in table" />
</div>

<div style={{display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'space-between' }}>
  ログ内のトレースリンクの例
  <img src={require('./images/trace_id_in_logs.png').default} class="image" alt="Trace links in logs" />
</div>

### データリンクの作成方法

あなたは、クエリ内で`traceID`という名前のカラムを選択することでデータリンクを作成できます。この名前は大文字と小文字を区別せず、"ID"の前にアンダースコアを追加することもサポートしています。例えば、`traceId`, `TraceId`, `TRACE_ID`, `tracE_iD`はすべて有効です。

OpenTelemetry が[ログ](#logs)または[トレース](#traces)クエリで有効になっている場合、トレース ID カラムは自動的に含まれます。

トレース ID カラムを含めることで、「**View Trace**」および「**View Logs**」リンクがデータに添付されます。

### リンクの機能

データリンクが存在すると、提供されたトレース ID を使用してトレースおよびログを開くことができます。

「**View Trace**」はトレース付きの分割パネルを開き、「**View Logs**」はトレース ID でフィルタリングされたログクエリを開きます。
リンクがダッシュボードではなくエクスプローラビューからクリックされた場合、リンクはエクスプローラビューの新しいタブで開かれます。

[ログ](./config.md#logs)と[トレース](./config.md#traces)の両方に対してデフォルトが構成されていることが、クエリタイプを跨ぐ（ログからトレース、トレースからログ）のに必要です。同じクエリタイプのリンクを開く場合には、ただクエリをコピーすることで済むため、デフォルトは必要ありません。

<div style={{display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'space-between' }}>
  ログクエリ（左パネル）からトレースを表示する例（右パネル）
  <img src={require('./images/demo_data_links.png').default} class="image" alt="Example of data links linking" />
</div>

## マクロ

マクロはクエリに動的 SQL を追加する簡単な方法です。
クエリが ClickHouse サーバーに送信される前に、プラグインはマクロを展開し、完全な式に置き換えます。

SQL エディタとクエリビルダーの両方のクエリでマクロを使用できます。

### マクロの使用方法

必要に応じて、クエリの任意の場所にマクロを複数回含めることができます。

以下は、`$__timeFilter` マクロを使用する例です：

入力：
```sql
SELECT log_time, log_message
FROM logs
WHERE $__timeFilter(log_time)
```

最終的なクエリ出力：
```sql
SELECT log_time, log_message
FROM logs
WHERE log_time >= toDateTime(1415792726) AND log_time <= toDateTime(1447328726)
```

この例では、Grafana ダッシュボードの時間範囲が `log_time` カラムに適用されています。

プラグインは中括弧 `{}` を使用した表記もサポートしています。クエリが [パラメーター](/docs/ja/sql-reference/syntax.md#defining-and-using-query-parameters)内に必要な場合は、この表記を使用します。

### マクロのリスト

プラグインで利用可能なすべてのマクロのリストです：

| マクロ                                   | 説明                                                                                                                                                             | 出力例                                                                                                                                |
| -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `$__dateFilter(columnName)`                  | Grafana パネルの時間範囲を[Date](/docs/ja/sql-reference/data-types/date.md)として指定されたカラムにフィルタリングする条件に置き換えます。                                 | `columnName >= toDate('2022-10-21') AND columnName <= toDate('2022-10-23')`                                       |
| `$__timeFilter(columnName)`                  | Grafana パネルの時間範囲を[DateTime](/docs/ja/sql-reference/data-types/datetime.md)として指定されたカラムにフィルタリングする条件に置き換えます。                         | `columnName >= toDateTime(1415792726) AND time <= toDateTime(1447328726)`                                         |
| `$__timeFilter_ms(columnName)`               | Grafana パネルの時間範囲を[DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)として指定されたカラムにフィルタリングする条件に置き換えます。                     | `columnName >= fromUnixTimestamp64Milli(1415792726123) AND columnName <= fromUnixTimestamp64Milli(1447328726456)` |
| `$__dateTimeFilter(dateColumn, timeColumn)`  | `$__dateFilter()` と `$__timeFilter()` を別々の Date と DateTime カラムで組み合わせたショートハンドです。Alias `$__dt()`                                                                               | `$__dateFilter(dateColumn) AND $__timeFilter(timeColumn)`                                             |
| `$__fromTime`                                | Grafana パネルの範囲の開始時刻を[DateTime](/docs/ja/sql-reference/data-types/datetime.md)としてキャストしたものに置き換えます。                                                     | `toDateTime(1415792726)`                                                                                          |
| `$__fromTime_ms`                             | パネル範囲の開始時刻を[DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)としてキャストしたものに置き換えます。                                                         | `fromUnixTimestamp64Milli(1415792726123)`                                                                         |
| `$__toTime`                                  | Grafana パネル範囲の終了時刻を[DateTime](/docs/ja/sql-reference/data-types/datetime.md)としてキャストしたものに置き換えます。                                                       | `toDateTime(1447328726)`                                                                                          |
| `$__toTime_ms`                               | パネル範囲の終了時刻を[DateTime64](/docs/ja/sql-reference/data-types/datetime64.md)としてキャストしたものに置き換えます。                                                           | `fromUnixTimestamp64Milli(1447328726456)`                                                                         |
| `$__timeInterval(columnName)`                | ウィンドウサイズに基づく間隔を計算する関数で置き換えます。                                                                                                    | `toStartOfInterval(toDateTime(columnName), INTERVAL 20 second)`                                                   |
| `$__timeInterval_ms(columnName)`             | ウィンドウサイズに基づく間隔を計算する関数で置き換えます（ミリ秒単位）。                                                                                               | `toStartOfInterval(toDateTime64(columnName, 3), INTERVAL 20 millisecond)`                                         |
| `$__interval_s`                              | ダッシュボード間隔を秒単位で置き換えます。                                                                                                                                      | `20`                                                                                                              |
| `$__conditionalAll(condition, $templateVar)` | テンプレート変数がすべての値を選択していない場合は第一引数に置き換えます。テンプレート変数がすべての値を選択する場合は `1=1` に置き換えます。 | `condition` または `1=1`                                                                                           |

