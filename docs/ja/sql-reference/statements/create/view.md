---
slug: /ja/sql-reference/statements/create/view
sidebar_position: 37
sidebar_label: VIEW
---

# CREATE VIEW

新しいビューを作成します。ビューは[通常のビュー](#normal-view)、[Materialized View](#materialized-view)、[リフレッシュ可能なMaterialized View](#refreshable-materialized-view)、および[ウィンドウビュー](#window-view-experimental)（リフレッシュ可能なMaterialized Viewとウィンドウビューはエクスペリメンタル機能です）であることができます。

## 通常のビュー

構文:

``` sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

通常のビューはデータを保存しません。アクセスするたびに他のテーブルからの読み取りを実行します。つまり、通常のビューは保存されたクエリにすぎません。ビューから読み取ると、この保存されたクエリが[FROM](../../../sql-reference/statements/select/from.md)句内のサブクエリとして使用されます。

例として、次のビューを作成したとします。

``` sql
CREATE VIEW view AS SELECT ...
```

そして、クエリを記述しました。

``` sql
SELECT a, b, c FROM view
```

このクエリは、次のサブクエリを使用するのと完全に同等です。

``` sql
SELECT a, b, c FROM (SELECT ...)
```

## パラメータ化ビュー

パラメータ化ビューは通常のビューに似ていますが、すぐには解決されないパラメータを持って作成することができます。これらのビューは、ビューの名前を関数名として、パラメータの値を引数として指定するテーブル関数で使用できます。

``` sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

上記は、下記のようにパラメータを代入することでテーブル関数として使用できるテーブルのビューを作成します。

``` sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

## マテリアライズドビュー

``` sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name] [ENGINE = engine] [POPULATE]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

:::tip
[Materialized View](docs/en/guides/developer/cascading-materialized-views.md)を使用するステップバイステップガイドはこちらです。
:::

Materialized Viewは、対応する[SELECT](../../../sql-reference/statements/select/index.md)クエリによって変換されたデータを保存します。

`TO [db].[table]`なしでMaterialized Viewを作成する場合は、データ格納のためのテーブルエンジン`ENGINE`を指定しなければなりません。

`TO [db].[table]`でMaterialized Viewを作成する場合、`POPULATE`を同時に使用することはできません。

Materialized Viewは次のように実装されています: `SELECT`で指定されたテーブルにデータを挿入すると、挿入されたデータの一部がこの`SELECT`クエリによって変換され、その結果がビューに挿入されます。

:::note
ClickHouseのMaterialized Viewは挿入先テーブルへのデータ挿入時に**カラム名**を使用します。`SELECT`クエリ結果に存在しないカラム名がある場合、たとえカラムが[Nullable](../../data-types/nullable.md)でなくても、ClickHouseはデフォルト値を使用します。Materialized Viewを使用する際には、全てのカラムに別名を付けることが安全なプラクティスとされます。

ClickHouseのMaterialized Viewは、主に挿入トリガーのように実装されています。ビュークエリに集約がある場合、それは新しく挿入されたデータバッチにのみ適用されます。ソーステーブルの既存データへの変更（例えば、更新、削除、パーティション削除など）は、Materialized Viewを変更しません。

エラー発生時にClickHouseのMaterialized Viewは決定的な振る舞いを持ちません。つまり、すでに書き込まれたブロックは保存されますが、エラー後の全てのブロックは保存されません。

デフォルトでは一つのビューへのプッシュが失敗した場合、INSERTクエリも失敗し、いくつかのブロックはデータ格納先テーブルに書き込まれないかもしれません。これは`materialized_views_ignore_errors`設定を使用して変更できます（これは`INSERT`クエリのために設定する必要があります）。`materialized_views_ignore_errors=true`に設定すると、ビューへのプッシュ時の全てのエラーは無視され、全てのブロックはデータ格納先テーブルに書き込まれます。

また、デフォルトで`system.*_log`テーブルには`materialized_views_ignore_errors`が`true`に設定されています。
:::

`POPULATE`を指定すると、既存のテーブルデータがビュー作成時にビューに挿入されます。それはちょうど`CREATE TABLE ... AS SELECT ...`を実行するようなものです。それ以外の場合、ビューが作成された後にテーブルに挿入されたデータのみをクエリします。ビュー作成中にテーブルに挿入されたデータがビューに挿入されないため、`POPULATE`の使用を**おすすめしません**。

:::note
`POPULATE`が`CREATE TABLE ... AS SELECT ...`のように機能することを考えると、いくつかの制限があります:
- レプリケートされたデータベースでのサポートはありません
- ClickHouseクラウドでのサポートはありません

代わりに個別の`INSERT ... SELECT`を使用できます。
:::

`SELECT`クエリには`DISTINCT`、`GROUP BY`、`ORDER BY`、`LIMIT`を含めることができます。但し、対応する変換は挿入されたデータの各ブロックごとに独立して実行されることに注意してください。例えば、`GROUP BY`が設定されている場合、データは挿入中に集約されますが、挿入されたデータのパケット内でのみ適用されます。データはそれ以上集約されません。ただし、独立してデータ集約を行う`SummingMergeTree`のようなエンジンを使用する場合は例外です。

[ALTER](/docs/ja/sql-reference/statements/alter/view.md)クエリをMaterialized Viewに対して実行することには限界があります。例えば、`SELECT`クエリを更新することはできませんので、これが不便になる可能性があります。Materialized Viewが`TO [db.]name`の構文を使用している場合、ビューを`DETACH`してからターゲットテーブルに対して`ALTER`を実行し、その後に以前に`DETACH`したビューを`ATTACH`できます。

Materialized Viewは[optimize_on_insert](../../../operations/settings/settings.md#optimize-on-insert)設定によって影響を受けます。データはビューへの挿入前にマージされます。

ビューは通常のテーブルと同様に見えます。例えば、`SHOW TABLES`クエリの結果にリストされます。

ビューを削除するには、[DROP VIEW](../../../sql-reference/statements/drop.md#drop-view)を使用します。ただし、`DROP TABLE`もビューに対して機能します。

## SQLセキュリティ {#sql_security}

`DEFINER`と`SQL SECURITY`により、ビューの基礎となるクエリを実行するときに使用するClickHouseユーザーを指定できます。`SQL SECURITY`には、`DEFINER`、`INVOKER`、`NONE`の3つの合法な値があります。`DEFINER`句には、任意の既存のユーザーまたは`CURRENT_USER`を指定できます。

以下の表は、どのユーザーがビューから選択する権限が必要かを説明します。SQLセキュリティオプションに関係なく、ビューから読み取るためには`GRANT SELECT ON <view>`が必要であることに注意してください。

| SQLセキュリティオプション | ビュー                                                     | マテリアライズドビュー                                                                                              |
|-------------------------|--------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `DEFINER alice`         | `alice`はビューのソーステーブルに対する`SELECT`権限が必要です。| `alice`はビューのソーステーブルに対する`SELECT`権限とビューのターゲットテーブルに対する`INSERT`権限が必要です。 |
| `INVOKER`               | ユーザーはビューのソーステーブルに対する`SELECT`権限が必要です。| Materialized Viewには`SQL SECURITY INVOKER`を指定できません。                                                   |
| `NONE`                  | -                                                      | -                                                                                                                 |

:::note
`SQL SECURITY NONE`は廃止予定のオプションです。このオプションでビューを作成する権限を持つユーザーは、任意のクエリを実行することができます。したがって、このオプションでビューを作成するためには、`GRANT ALLOW SQL SECURITY NONE TO <user>`が必要です。
:::

`DEFINER`/`SQL SECURITY`が指定されていない場合、デフォルト値が使用されます:
- `SQL SECURITY`: 通常のビューには`INVOKER`、Materialized Viewには`DEFINER`（[設定で設定可能](../../../operations/settings/settings.md#default_normal_view_sql_security)）
- `DEFINER`: `CURRENT_USER`（[設定で設定可能](../../../operations/settings/settings.md#default_view_definer)）

`DEFINER`/`SQL SECURITY`が指定されずにビューがアタッチされた場合、Materialized Viewにはデフォルト値として`SQL SECURITY NONE`、通常のビューには`SQL SECURITY INVOKER`が使用されます。

既存のビューのSQLセキュリティを変更するには、以下を使用します。
```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

### 例
```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

## ライブビュー [廃止予定]

この機能は廃止予定であり、将来的に削除される予定です。

利便性のため、古いドキュメントは[こちら](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)にあります。

## リフレッシュ可能なMaterialized View [エクスペリメンタル] {#refreshable-materialized-view}

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name
REFRESH EVERY|AFTER interval [OFFSET interval]
RANDOMIZE FOR interval
DEPENDS ON [db.]name [, [db.]name [, ...]]
SETTINGS name = value [, name = value [, ...]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine] [EMPTY]
AS SELECT ...
[COMMENT 'comment']
```

ここで`interval`は単純な間隔のシーケンスです:

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

定期的に対応するクエリを実行し、その結果をテーブルに格納します。
 * クエリに`APPEND`が書かれている場合、各リフレッシュでは既存の行を削除せずに行をテーブルに挿入します。通常のINSERT SELECTと同様、挿入はアトミックではありません。
 * それ以外の場合、各リフレッシュはテーブルの以前の内容をアトミックに置き換えます。

通常の非リフレッシュ可能なMaterialized Viewとの違い:
 * 挿入トリガーがありません。すなわち、SELECTで指定されたテーブルに新しいデータが挿入されても、リフレッシュ可能なMaterialized Viewには自動的にはプッシュされません。定期的なリフレッシュがクエリ全体を実行します。
 * SELECTクエリに制限がありません。テーブル関数（例えば`url()`）、ビュー、UNION、JOINがすべて許可されます。

:::note
クエリの`REFRESH ... SETTINGS`部分にある設定はリフレッシュ設定（例: `refresh_retries`）であり、通常の設定（例: `max_threads`）とは異なります。通常の設定は、クエリの終了部分にある`SETTINGS`を使用して指定できます。
:::

### リフレッシュスケジュール

リフレッシュスケジュールの例:
```sql
REFRESH EVERY 1 DAY -- 毎日、午前0時（UTC）
REFRESH EVERY 1 MONTH -- 毎月1日、午前0時に
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- 毎月6日、午前2時に
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- 2週間ごとに土曜日、午後3:10に
REFRESH EVERY 30 MINUTE -- 00:00、00:30、01:00、01:30、などで
REFRESH AFTER 30 MINUTE -- 前のリフレッシュが完了してから30分後、日付に合わせて調整されません
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- AFTERではOFFSETは許可されていないため、構文エラー
REFRESH EVERY 1 WEEK 2 DAYS -- 9日ごと、特定の曜日や月に制約なし;
                            -- 特に、1969-12-29以降の日数が9で割り切れるとき
REFRESH EVERY 5 MONTHS -- 毎5ヶ月、毎年異なる月（12は5で割り切れないため）;
                       -- 特に、1970-01以降の月数が5で割り切れるとき
```

`RANDOMIZE FOR`は各リフレッシュの時間をランダムに調整します。例:
```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- 毎日午前1:30から2:30の間のランダムな時間
```

特定のビューについて、同時に一つのみのリフレッシュを実行可能です。例: `REFRESH EVERY 1 MINUTE`のビューがリフレッシュに2分かかる場合、2分ごとにリフレッシュします。その後、10秒でリフレッシュが完了する場合、1分ごとに戻ります。（特に、バックログのすべてをカバーするために10秒ごとにリフレッシュされることはありません - そのようなバックログはありません。）

さらに、`CREATE`クエリで作成されるとリフレッシュは直ちに開始されますが、`EMPTY`が指定されている場合は最初のリフレッシュはスケジュールに従って行われます。

### レプリケートデータベースでの動作

リフレッシュ可能なMaterialized Viewが[レプリケートデータベース](../../../engines/database-engines/replicated.md)にある場合、レプリカ同士で調整して、スケジュールされた時間に1つのレプリカのみがリフレッシュを実行するようになります。[ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md)テーブルエンジンが必要ですので、全てのレプリカがリフレッシュによって生成されたデータを参照できます。

`APPEND`モードでは、`SETTINGS全てのレプリカ=1`を使用して調整を無効にできます。これにより、レプリカは互いに独立してリフレッシュを実行します。この場合、ReplicatedMergeTreeは必要ありません。

非`APPEND`モードでは、調整されたリフレッシュのみがサポートされています。非調整されたリフレッシュには、`Atomic`データベースと`CREATE ... ON CLUSTER`クエリを使用して、すべてのレプリカでリフレッシュ可能なMaterialized Viewを作成します。

調整はKeeperによって行われます。znodeパスは[default_replica_path](../../../operations/server-configuration-parameters/settings.md#default_replica_path)サーバー設定によって決定されます。

### 依存関係 {#refresh-dependencies}

`DEPENDS ON`は異なるテーブルのリフレッシュを同期します。たとえば、次のような2つのリフレッシュ可能なMaterialized Viewのチェーンがあるとします:
```sql
CREATE MATERIALIZED VIEW source REFRESH EVERY 1 DAY AS SELECT * FROM url(...)
CREATE MATERIALIZED VIEW destination REFRESH EVERY 1 DAY AS SELECT ... FROM source
```
`DEPENDS ON`なしでは、両方のビューが真夜中にリフレッシュを開始し、`destination`は通常、`source`の昨日のデータを参照します。依存関係を追加すると:
```
CREATE MATERIALIZED VIEW destination REFRESH EVERY 1 DAY DEPENDS ON source AS SELECT ... FROM source
```
`destination`のリフレッシュは、`source`の同日のリフレッシュが完了した後にのみ開始されるので、`destination`は新鮮なデータに基づくことになります。

または、同じ結果を以下で達成できます:
```
CREATE MATERIALIZED VIEW destination REFRESH AFTER 1 HOUR DEPENDS ON source AS SELECT ... FROM source
```
ここでの`1 HOUR`は`source`のリフレッシュ期間より短ければ任意の長さに設定できます。依存するテーブルは、依存するテーブルのリフレッシュ率より頻繁にリフレッシュされることはありません。これは、リフレッシュ可能なビューのチェーンを実際のリフレッシュ期間を指定することなく設定するための有効な方法です。

いくつかのさらなる例:
 * `REFRESH EVERY 1 DAY OFFSET 10 MINUTE`（`destination`）が`REFRESH EVERY 1 DAY`（`source`）に依存<br/>
   `source`のリフレッシュが10分以上かかる場合、`destination`は待機します。
 * `REFRESH EVERY 1 DAY OFFSET 1 HOUR`が`REFRESH EVERY 1 DAY OFFSET 23 HOUR`に依存<br/>
   上記と同様に、対応するリフレッシュが異なるカレンダー日に起こるにもかかわらず。
   `destination`のリフレッシュは日X+1の日に始まり、`source`の日Xのリフレッシュを（それが2時間以上かかる場合）待機します。
 * `REFRESH EVERY 2 HOUR`が`REFRESH EVERY 1 HOUR`に依存<br/>
   2時間ごとのリフレッシュは、一時間ごとのリフレッシュの後に行われます。例えば、真夜中のリフレッシュの後、次に午前2時のリフレッシュの後等。
 * `REFRESH EVERY 1 MINUTE`が`REFRESH EVERY 2 HOUR`に依存<br/>
   `REFRESH AFTER 1 MINUTE`が`REFRESH EVERY 2 HOUR`に依存<br/>
   `REFRESH AFTER 1 MINUTE`が`REFRESH AFTER 2 HOUR`に依存<br/>
   `destination`は`source`の各リフレッシュ後に一度リフレッシュされます。すなわち、2時間ごと。`1 MINUTE`は実質的に無視されます。
 * `REFRESH AFTER 1 HOUR`が`REFRESH AFTER 1 HOUR`に依存<br/>
   現在、これは推奨されていません。

:::note
`DEPENDS ON`はリフレッシュ可能なMaterialized View間でのみ機能します。通常のテーブルを`DEPENDS ON`リストに記載すると、ビューが一切リフレッシュされなくなります（依存関係は`ALTER`で削除できます、下記参照）。
:::

### 設定

利用可能なリフレッシュ設定:
 * `refresh_retries` - リフレッシュクエリが例外を伴って失敗した場合に再試行する回数。すべての再試行に失敗した場合、次のスケジュールされたリフレッシュ時間にスキップします。0は再試行なしを意味し、-1は無限の再試行を意味します。デフォルト: 0。
 * `refresh_retry_initial_backoff_ms` - `refresh_retries`がゼロでない場合、最初の再試行前の遅延。各再試行ごとに遅延が倍増し、`refresh_retry_max_backoff_ms`まで続きます。デフォルト: 100 ms。
 * `refresh_retry_max_backoff_ms` - リフレッシュ試行間の遅延の指数増加の限界。デフォルト: 60000 ms（1分）。

### リフレッシュパラメータの変更 {#changing-refresh-parameters}

リフレッシュパラメータを変更するには:
```
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

:::note
これは全リフレッシュパラメータ、スケジュール、依存関係、設定、およびAPPEND性を一度に置き換えます。例: テーブルに`DEPENDS ON`があった場合、`MODIFY REFRESH`を`DEPENDS ON`なしで実行すると依存関係が削除されます。
:::

### その他の操作

すべてのリフレッシュ可能なMaterialized Viewのステータスは、[`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md)テーブルで利用可能です。具体的には、リフレッシュの進行状況（実行中の場合）、最後と次回のリフレッシュ時間、失敗した場合の例外メッセージが含まれています。

リフレッシュを手動で停止、開始、トリガー、またはキャンセルするには、[`SYSTEM STOP|START|REFRESH|CANCEL VIEW`](../system.md#refreshable-materialized-views)を使用します。

リフレッシュが完了するのを待つには、[`SYSTEM WAIT VIEW`](../system.md#refreshable-materialized-views)を使用します。特に、リフレッシュ可能なMaterialized Viewを作成した後の初期リフレッシュを待機するのに便利です。

:::note
豆知識: リフレッシュされているビューから前のバージョンのデータを読み取ることが許可されています。これで、コンウェイのライフゲームを実装できます: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

## ウィンドウビュー [エクスペリメンタル]

:::info
これは将来のリリースで後方互換性がない形で変更される可能性があるエクスペリメンタルな機能です。ウィンドウビューと`WATCH`クエリの使用を許可するには、[allow_experimental_window_view](../../../operations/settings/settings.md#allow-experimental-window-view)設定を使用して`set allow_experimental_window_view = 1`を入力してください。
:::

``` sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

ウィンドウビューはデータを時間ウィンドウ別に集約し、ウィンドウが準備が整ったときに結果を出力できます。中間集計結果を内部 (もしくは指定した) テーブルに保存してレイテンシを削減し、指定されたテーブルへのプッシュや`WATCH`クエリを使用して通知をプッシュすることができます。

ウィンドウビューの作成は`MATERIALIZED VIEW`の作成に似ています。ウィンドウビューは中間データを保存するために内部ストレージエンジンを必要とします。内部ストレージは`INNER ENGINE`句を使用して指定できます。ウィンドウビューはデフォルトの内部エンジンとして`AggregatingMergeTree`を使用します。

`TO [db].[table]`なしでウィンドウビューを作成する場合、データを保存するテーブルエンジン`ENGINE`を指定しなければなりません。

### 時間ウィンドウ関数

[時間ウィンドウ関数](../../functions/time-window-functions.md)は、レコードの下限と上限ウィンドウを取得するために使用されます。ウィンドウビューは時間ウィンドウ関数と一緒に使用する必要があります。

### タイムアトリビュート

ウィンドウビューは**処理時間**と**イベント時間**のプロセスをサポートします。

**処理時間**は、ウィンドウビューがローカルのマシンの時間に基づいて結果を生成できるようにします。これはデフォルトで使用され、最も基本的な時間概念ですが、決定性を提供しません。処理時間アトリビュートは、時間ウィンドウ関数の`time_attr`をテーブルカラムに設定するか、または関数`now()`を使用して定義できます。次のクエリは処理時間を使用したウィンドウビューを作成します。

``` sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**イベント時間**は、各イベントがその制作装置で発生した時間です。この時間は、通常、レコードが生成されたときにその中に埋め込まれています。イベント時間処理は、順序が乱れたイベントや遅延イベントでも一貫した結果を得ることができます。ウィンドウビューは、`WATERMARK`構文を使用してイベント時間処理をサポートしています。

ウィンドウビューは、3つのウォーターマーク戦略を提供します:

* `STRICTLY_ASCENDING`: これまでに観察された最大のタイムスタンプのウォーターマークを発行します。最大タイムスタンプより小さいタイムスタンプを持つ行は遅れていません。
* `ASCENDING`: これまでに観察された最大のタイムスタンプから1を引いたウォーターマークを発行します。最大タイムスタンプと等しいか小さいタイムスタンプを持つ行は遅れていません。
* `BOUNDED`: WATERMARK=INTERVAL。指定された遅延を引いた最大のタイムスタンプのウォーターマークを発行します。

以下のクエリは、`WATERMARK`を使用してウィンドウビューを作成する例です:

``` sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

デフォルトでは、ウォーターマークが到達するとウィンドウが発生しますが、ウォーターマークを過ぎて到着した要素は削除されます。ウィンドウビューは`ALLOWED_LATENESS=INTERVAL`を設定して遅れたイベントの処理をサポートします。遅延処理の例は次のとおりです:

``` sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

遅延で発行された要素は、以前の計算の更新結果として扱われるべきであることに注意してください。ウィンドウが終了したときではなく、遅延したイベントが到着したときにすぐにウィンドウが発生します。したがって、同じウィンドウに対して複数の出力が生じます。ユーザーはこれらの重複した結果を考慮するか、重複を排除する必要があります。

ウィンドウビューで指定された`SELECT`クエリを`ALTER TABLE ... MODIFY QUERY`文を使用して変更できます。ビューをウィンドウビューとともに使用する場合、または`TO [db.]name`句とともに使用する場合、新しい`SELECT`クエリの結果となるデータ構造は元の`SELECT`クエリと同じでなければなりません。現在のウィンドウ内のデータは失われます。これは中間状態を再利用できないためです。

### 新しいウィンドウの監視

ウィンドウビューは、[WATCH](../../../sql-reference/statements/watch.md)クエリをサポートして変更を監視するか、`TO`構文を使用して結果をテーブルに出力します。

``` sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

`WATCH`クエリは`LIVE VIEW`と同様に動作します。`LIMIT`を指定することで、クエリを終了する前に受信する更新の数を設定できます。`EVENTS`句を使用して`WATCH`クエリの短い形式を取得することができ、クエリ結果の代わりに最新のクエリウォーターマークを取得します。

### 設定

- `window_view_clean_interval`: 古くなったデータを解放するためのウィンドウビューのクリーニング間隔（秒）。システムはシステム時間や`WATERMARK`設定に従って完全にトリガーされていないウィンドウを保持し、他のデータは削除されます。
- `window_view_heartbeat_interval`: ウォッチクエリがアクティブであることを示す心拍間隔（秒）。
- `wait_for_window_view_fire_signal_timeout`: イベント時間処理中にウィンドウビューの発火シグナルを待機するタイムアウト。

### 例

例えば、`data`というログテーブルで10秒ごとにクリックログの数をカウントする必要があるとします。そのテーブル構造は以下の通りです。

``` sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

まず、10秒間隔のウィンドウを持つウィンドウビューを作成します。

``` sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

次に、`WATCH`クエリを使って結果を取得します。

``` sql
WATCH wv
```

ログがテーブル`data`に挿入されると、

``` sql
INSERT INTO data VALUES(1,now())
```

`WATCH`クエリは次のように結果を表示するはずです。

``` text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

または、`TO`構文を使用して出力を別のテーブルに関連付けることができます。

``` sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

追加の例は、ClickHouseのステートフルテストの中で見つけることができます（それらは`*window_view*`とされています）。

### ウィンドウビューの使用

ウィンドウビューは以下のシナリオで便利です:

* **監視**: 時間単位でメトリクスログを集約・計算し、結果をターゲットテーブルに出力します。ダッシュボードはターゲットテーブルをソーステーブルとして使用できます。
* **分析**: 時間ウィンドウでデータを自動的に集約し、前処理する。この処理は大量のログを分析する際に役立ちます。前処理によって複数のクエリでの反復計算が排除され、クエリの遅延が減少します。

## 関連コンテンツ

- ブログ: [ClickHouseで時系列データを扱う](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- ブログ: [ClickHouseを使った観測性ソリューションの構築 - Part 2 - トレース](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
