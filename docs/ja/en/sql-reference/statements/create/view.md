---
description: 'CREATE VIEW に関するドキュメント'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="create-view">
  # CREATE VIEW
</div>

新しいビューを作成します。ビューには、[通常ビュー](#normal-view)、[マテリアライズドビュー](#materialized-view)、[リフレッシュ可能なマテリアライズドビュー](#refreshable-materialized-view)、および[ウィンドウビュー](/ja/sql-reference/statements/create/view#window-view)があります。

<div id="normal-view">
  ## 通常ビュー
</div>

構文:

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

通常のビューはデータを一切保存しません。アクセスされるたびに、別のテーブルから読み取るだけです。つまり、通常のビューは単なる保存クエリにすぎません。ビューを参照すると、この保存クエリが [FROM](../../../sql-reference/statements/select/from.md) 句のサブクエリとして使われます。

例として、ビューを作成したとします。

```sql
CREATE VIEW view AS SELECT ...
```

そして、クエリを記述します:

```sql
SELECT a, b, c FROM view
```

このクエリは、次のサブクエリを使用した場合と完全に同等です。

```sql
SELECT a, b, c FROM (SELECT ...)
```

<div id="parameterized-view">
  ## パラメーター化ビュー
</div>

パラメーター化ビューは通常のビューに似ていますが、すぐには評価されないパラメーターを指定して作成できます。これらのビューはテーブル関数として使用でき、その場合はビュー名を関数名として、パラメーター値を引数として指定します。

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

上記により、以下のようにパラメータを置き換えることでテーブル関数として使用できる、テーブル用のビューが作成されます。

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

<div id="materialized-view">
  ## Materialized View
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` と `IF NOT EXISTS` は同時に使用できません。組み合わせると構文エラーになります。

<div id="create-or-replace-materialized-view">
  ### CREATE OR REPLACE MATERIALIZED VIEW
</div>

`CREATE OR REPLACE MATERIALIZED VIEW` は、既存のmaterialized view と、その内部ストレージテーブル (存在する場合) をアトミックに置き換えます。この操作には、`Atomic` または `Replicated` データベースエンジンが必要です。

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

主な動作:

* **`TO` 句なし**: 古い内部テーブルが削除され、新しいテーブルが作成されます。`POPULATE` が指定されていない限り、内部テーブル内の既存データは失われます。
* **`TO` 句あり**: 置き換えられるのはビュー定義のみです。ターゲットテーブルとそのデータには影響ありません。
* `REFRESH`、`ON CLUSTER`、およびすべてのエンジンオプションと互換性があります。`POPULATE` は `Atomic` データベースでのみサポートされ、`Replicated` データベースでは拒否されます (以下の `POPULATE` に関する注記を参照) 。
* `CREATE VIEW` および `DROP VIEW` 権限が必要です。

:::note
`CREATE OR REPLACE MATERIALIZED VIEW` は、`Atomic` または `Replicated` データベースエンジンでのみサポートされます。`Ordinary` データベースエンジンではサポートされません。
:::

**例:**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
[Materialized views](/ja/guides/developer/cascading-materialized-views.md) の使用方法については、こちらのステップバイステップガイドを参照してください。
:::

materialized view には、対応する [SELECT](../../../sql-reference/statements/select/index.md) クエリで変換されたデータが格納されます。

`TO [db].[table]` を付けずに materialized view を作成する場合は、データの格納に使用する table engine である `ENGINE` を指定する必要があります。

`TO [db].[table]` を付けて materialized view を作成する場合、`POPULATE` は併用できません。

materialized view は次のように実装されています。`SELECT` で指定された table にデータを挿入すると、挿入されたデータの一部がこの `SELECT` クエリによって変換され、その結果が view に挿入されます。

:::note
ClickHouse の materialized view では、宛先テーブルへの挿入時にカラム順ではなく **カラム名** が使用されます。`SELECT` クエリの結果に一部のカラム名が含まれていない場合、そのカラムが [Nullable](../../data-types/nullable.md) でなくても、ClickHouse はデフォルト値を使用します。安全な方法として、materialized view を使用する際は各カラムに別名を付けることを推奨します。

ClickHouse の materialized view は、insert trigger に近い形で実装されています。view のクエリに aggregation がある場合、それは新たに挿入されたデータのバッチに対してのみ適用されます。source table の既存データに対する変更 (update、delete、drop partition など) は、materialized view には反映されません。

ClickHouse の materialized view は、error 発生時の動作が決定論的ではありません。つまり、すでに書き込まれた block は宛先テーブルに保持されますが、error 発生後の block は保持されません。

デフォルトでは、いずれかの view への push で例外が発生すると、`INSERT` クエリは失敗します。その時点で block がすでに source table に到達しているかどうかは保証されません。これは view の error ではなく、insert pipeline のタイミングに依存します。失敗した `INSERT` は、挿入の重複排除 (`insert_deduplicate`, `deduplicate_blocks_in_dependent_materialized_views`) を有効にして再試行し、source table とすべての dependent views に exactly-once で配信されるようにしてください。

`INSERT` クエリで `materialized_views_ignore_errors=true` を設定しても、変わるのはエラーの報告方法だけです。各ビューのエラーは警告として記録され、`INSERT` クエリ自体は成功します。失敗したビューの宛先への配信は部分的になり、例外が発生する前に処理されたブロックは保持されますが、失敗したブロックとそれ以降のブロックはそのビューでは破棄されます。その宛先の下流にあるビューでも、到達したブロックしか見えないため、配信はやはり部分的になります。一方、例外が発生しなかった兄弟ビュー (およびその下流の連鎖) には完全に書き込まれ、ソーステーブルにも通常どおり書き込まれます。`INSERT` は成功として報告されるため、クライアントは失敗シグナルを受け取らず、自動的な再試行もトリガーされません。この設定は、ソーステーブルへの書き込みをビュー側の問題で妨げてはならない場合にのみ使用してください (たとえば `system.*_log` テーブル) 。

`materialized_views_ignore_errors` は `system.*_log` テーブルではデフォルトで `true` です。
:::

`POPULATE` を指定すると、既存のテーブルデータが、`CREATE TABLE ... AS SELECT ...` を実行するのと同様に、ビューの作成時にビューへ挿入されます。指定しない場合、クエリに含まれるのはビュー作成後にテーブルへ挿入されたデータだけです。ビュー作成中にテーブルへ挿入されたデータはビューには挿入されないため、`POPULATE` の使用は **推奨しません**。

:::note
`POPULATE` は `CREATE TABLE ... AS SELECT ...` のように動作するため、次の制限があります。

* Replicated database ではサポートされていません
* ClickHouse Cloud ではサポートされていません

代わりに、別途 `INSERT ... SELECT` を使用できます。
:::

`SELECT` クエリには `DISTINCT`、`GROUP BY`、`ORDER BY`、`LIMIT` を含めることができます。対応する変換は、挿入された各データブロックごとに独立して実行される点に注意してください。たとえば `GROUP BY` が設定されている場合、データは挿入時に集計されますが、それは挿入されたデータの単一のパケット内に限られます。その後、データがさらに集計されることはありません。例外は、`SummingMergeTree` のように、データ集計を独自に実行する `ENGINE` を使用する場合です。

materialized view が `TO [db.]name` 構文を使用している場合は、そのビューを `DETACH` し、ターゲットテーブルに対して `ALTER` を実行したあと、先ほど `DETACH` したビューを `ATTACH` できます。

materialized view は [optimize&#95;on&#95;insert](/ja/operations/settings/settings#optimize_on_insert) 設定の影響を受けることに注意してください。データはビューへ挿入される前にマージされます。

ビューは通常のテーブルと同じように見えます。たとえば、`SHOW TABLES` クエリの結果に表示されます。

ビューを削除するには、[DROP VIEW](../../../sql-reference/statements/drop.md#drop-view) を使用します。なお、VIEW に対しては `DROP TABLE` も機能します。

<div id="sql_security">
  ## SQL security
</div>

`DEFINER` と `SQL SECURITY` を使用すると、ビューの基になるクエリの実行時にどの ClickHouse ユーザーを使用するかを指定できます。
`SQL SECURITY` には、`DEFINER`、`INVOKER`、`NONE` の 3 つの有効な値があります。`DEFINER` 句では、既存の任意のユーザーまたは `CURRENT_USER` を指定できます。

次の表は、ビューを SELECT する際に、どのユーザーにどの権限が必要かを示しています。
SQL security オプションにかかわらず、いずれの場合も、ビューから読み取るには `GRANT SELECT ON <view>` が引き続き必要である点に注意してください。

| SQL security option | View                                                | Materialized View                                                                  |
| ------------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------- |
| `DEFINER alice`     | `alice` は、ビューのソーステーブルに対する `SELECT` 権限を持っている必要があります。 | `alice` は、ビューのソーステーブルに対する `SELECT` 権限と、ビューのターゲットテーブルに対する `INSERT` 権限を持っている必要があります。 |
| `INVOKER`           | ユーザーは、ビューのソーステーブルに対する `SELECT` 権限を持っている必要があります。     | materialized view には `SQL SECURITY INVOKER` を指定できません。                              |
| `NONE`              | -                                                   | -                                                                                  |

:::note
`SQL SECURITY NONE` は非推奨のオプションです。`SQL SECURITY NONE` を指定してビューを作成する権限を持つユーザーは、任意のクエリを実行できてしまいます。
そのため、このオプションでビューを作成するには `GRANT ALLOW SQL SECURITY NONE TO <user>` が必要です。
:::

`DEFINER`/`SQL SECURITY` が指定されていない場合は、デフォルト値が使用されます。

* `SQL SECURITY`: 通常のビューでは `INVOKER`、materialized view では `DEFINER` ([設定で変更可能](../../../operations/settings/settings.md#default_normal_view_sql_security))
* `DEFINER`: `CURRENT_USER` ([設定で変更可能](../../../operations/settings/settings.md#default_view_definer))

`DEFINER`/`SQL SECURITY` を指定せずにビューがアタッチされた場合、デフォルト値は materialized view では `SQL SECURITY NONE`、通常のビューでは `SQL SECURITY INVOKER` です。

既存のビューの SQL security を変更するには、次を使用します。

```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

<div id="examples">
  ### 例
</div>

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

<div id="live-view">
  ## Live View
</div>

<DeprecatedBadge />

この機能は非推奨であり、今後削除される予定です。

以前のドキュメントは[こちら](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)にあります。

<div id="refreshable-materialized-view">
  ## リフレッシャブルmaterialized view
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

ここで、`interval` は単純なインターバルからなる数列です:

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

`REFRESH` 句では、`EVERY`、`AFTER`、`DEPENDS ON` のうち少なくとも 1 つを指定する必要があります。これらを 1 つも伴わない単独の `REFRESH` は拒否されます。`EVERY`/`AFTER` なしの `REFRESH DEPENDS ON ...` は、`REFRESH AFTER 0 SECOND DEPENDS ON ...` の省略記法です。詳細は下の [Refresh Dependencies](#refresh-dependencies) を参照してください。

対応するクエリを定期的に実行し、その結果をテーブルに格納します。

* `APPEND` が指定されている場合、各 refresh では既存の行を削除せずにテーブルへ行を挿入します。この insert は、通常の `INSERT INTO ... SELECT` クエリと同様にアトミックではありません。
* それ以外の場合、各 refresh はテーブルの以前の内容をアトミックに置き換えます。

通常の非リフレッシャブルmaterialized view との違い:

* insert trigger はありません。`SELECT` で指定されたテーブルに新しいデータが insert されても、それが自動的にリフレッシャブルmaterialized view に反映されることは *ありません*。代わりに、データの挿入は定期 refresh または手動 refresh の実行時にのみ行われます。
* `SELECT` クエリに制限はありません。Table functions (例: `url()`) 、views、UNION、JOIN はすべて使用できます。

:::note
クエリの `REFRESH ... SETTINGS` 部分にある設定は refresh settings (例: `refresh_retries`) であり、通常の設定 (例: `max_threads`) とは異なります。通常の設定は、クエリ末尾の `SETTINGS` を使って指定できます。
:::

<div id="refresh-schedule">
  ### リフレッシュスケジュール
</div>

リフレッシュスケジュールの例:

```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` は各リフレッシュの時刻をランダムに調整します。例:

```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

特定のビューでは、同時に実行できる refresh は最大 1 つまでです。たとえば、`REFRESH EVERY 1 MINUTE` を指定したビューの refresh に 2 分かかる場合、実際の refresh 間隔は 2 分ごとになります。その後、処理が高速化して 10 秒で refresh できるようになれば、再び 1 分ごとの refresh に戻ります。 (特に、実行されなかった refresh のたまりを取り戻すために 10 秒ごとに refresh されることはありません。そのようなたまりは存在しません。)

通常、最初の refresh は materialized view の作成直後に開始されます。最後の refresh からの経過時間は無限大であるため、どの schedule でも「今すぐ refresh すべき時刻」と判断されるからです。`EMPTY` が指定されている場合、この初回 refresh はスキップされ、最初の refresh は次の schedule 時刻に実行されます。たとえば、`EVERY 1 HOUR` の場合、最初の refresh は現在の時刻の属する時間の終わりに実行されます。

<div id="in-replicated-db">
  ### Replicated DB 内の場合
</div>

リフレッシャブルmaterialized view が [Replicated database](../../../engines/database-engines/replicated.md) 内にある場合、各レプリカは互いに協調し、スケジュールされた時刻ごとに 1 つのレプリカだけがリフレッシュを実行します。リフレッシュで生成されたデータをすべてのレプリカが参照できるようにするため、[ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) テーブルエンジンが必要です。

`APPEND` モードでは、`SETTINGS all_replicas = 1` を使用して協調を無効にできます。これにより、各レプリカは互いに独立してリフレッシュを実行します。この場合、ReplicatedMergeTree は不要です。

`APPEND` 以外のモードでは、協調されたリフレッシュのみがサポートされます。協調なしで行うには、`Atomic` データベースと `CREATE ... ON CLUSTER` クエリを使用して、すべてのレプリカ上にリフレッシャブルmaterialized view を作成してください。

協調は Keeper を通じて行われます。znode パスは [default&#95;replica&#95;path](../../../operations/server-configuration-parameters/settings.md#default_replica_path) サーバー設定によって決まります。

<div id="refresh-dependencies">
  ### リフレッシュの依存関係
</div>

`DEPENDS ON` は、異なるテーブルのリフレッシュのタイミングを同期します:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```

依存するビューのリフレッシュは、依存先のすべてのビューのリフレッシュが完了してからでないと開始されません。

別のビューのリフレッシュ直後にリフレッシュするには：

```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```

あるいは、次のように書くこともできます:

```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` は、リフレッシュ可能なマテリアライズドビュー間でのみ機能します。特に、依存先のビューで `TO <table>` を使用している場合は、テーブル名ではなくビュー名を使ってください。`DEPENDS ON` のリストに通常のテーブルやリフレッシュ可能でないビューが含まれている場合、またはタイプミスがある場合、そのビューは更新されず、`system.view_refreshes` では状態 `MissingDependencies` と表示されます。依存関係は `ALTER` を使って変更または削除できます。詳しくは [リフレッシュパラメータの変更](#changing-refresh-parameters) を参照してください。
:::

<div id="using-depends-on-for-consistent-propagation-latency">
  #### 一貫した伝播レイテンシのために `DEPENDS ON` を使用する
</div>

両方のビューが同じ周期で `REFRESH EVERY` を使用している場合、依存関係は各タイムスロットに適用されます。

たとえば、ビュー X と Y の両方が `REFRESH EVERY 1 HOUR` を使用し、Y が X の出力テーブルを読み取るとします。依存関係がない場合、Y から見えるのは通常、X の前の時間帯の更新で作成されたデータです。`DEPENDS ON X` を指定すると、Y の 11:00 の更新は、X の 11:00 の更新が完了してから初めて開始されます。

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

依存元と依存先はどちらも、リフレッシュの実行時間がリフレッシュ間隔を超えると、それぞれ個別にタイムスロットをスキップすることがあります。依存先のリフレッシュが、依存元のリフレッシュ1回ごとに必ず1回だけ実行されるとは限りません。

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

<div id="using-depends-on-for-batched-stream-processing">
  #### バッチ化されたストリーム処理で `DEPENDS ON` を使用する
</div>

`REFRESH EVERY` を使用しない場合、依存するビュー X は、X の前回のリフレッシュ以降にそのすべての依存先が少なくとも 1 回リフレッシュされるとリフレッシュされます。`REFRESH AFTER T` は遅延を追加します。依存するビューは、依存先のリフレッシュ完了から T 時間後にリフレッシュを開始します。

循環依存は許可されており、有用です。次のリフレッシュ可能なマテリアライズドビュー のグラフを考えてみましょう。

1. X はある stream から行のバッチを取り込み、それらをテーブルに格納します。
2. 次に、Y と Z はどちらもそのテーブルを読み取り、それぞれ異なる集約を行って、結果を別のテーブルに追記します。
3. バッチが完全に処理されると、X は次のバッチを取得し、この cycle が繰り返されます。

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

全体の例：

```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

より長いチェーンでも機能します。

これは、リフレッシュの協調が有効な場合、つまりビューが Replicated または Shared データベースにある場合にのみ、うまく機能します。協調がないと、サーバーの再起動によってこのサイクルが途切れるため、ビューの作成後に一度だけではなく、再起動のたびに手動で `SYSTEM REFRESH VIEW` を実行する必要があります。

<div id="refresh-settings">
  ### リフレッシュ設定
</div>

利用可能なリフレッシュ設定は次のとおりです。

* `refresh_retries` - リフレッシュ クエリが例外により失敗した場合の再試行回数です。すべての再試行が失敗した場合は、次にスケジュールされたリフレッシュ時刻までスキップします。0 は再試行なし、-1 は無限に再試行することを意味します。デフォルト: 2。
* `refresh_retry_initial_backoff_ms` - `refresh_retries` が 0 でない場合の、最初の再試行までの待機時間です。以降の再試行では、待機時間が毎回 2 倍になり、`refresh_retry_max_backoff_ms` まで増加します。デフォルト: 100 ms。
* `refresh_retry_max_backoff_ms` - リフレッシュ試行の間隔が指数的に増加する際の上限です。デフォルト: 60000 ms (1 分) 。
* `all_replicas` - `APPEND` を使用する [Replicated database](../../../engines/database-engines/replicated.md) で、すべてのレプリカが個別にリフレッシュするか、各スケジュール時刻に 1 つのレプリカのみがリフレッシュするかを制御します。ビューの作成後は変更できません。デフォルト: `false`。

<div id="changing-refresh-parameters">
  ### リフレッシュパラメーターの変更
</div>

既存のリフレッシャブルmaterialized viewのリフレッシュパラメーターは、[`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement) を使用して変更します。

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

スケジュール (`EVERY` または `AFTER`) は必須です。このステートメントでは、スケジュール、`RANDOMIZE FOR`、`DEPENDS ON`、およびリフレッシュ設定を含む*すべての*リフレッシュ パラメータが、常に指定した内容で置き換えられます。省略したものは、デフォルトに戻される (設定の場合) か、削除されます (依存関係、ランダム化の場合) 。

:::note

* リフレッシュ設定のみ (たとえば `refresh_retries`) を変更するには、既存のスケジュールも再度指定します。

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

* `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` は materialized view ではサポートされていないため、`MODIFY REFRESH` を使用する必要があります。

* `APPEND` の追加または削除はサポートされていません。

* `all_replicas` 設定は、作成後に変更できません。
  :::

例:

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

<div id="other-operations">
  ### その他の操作
</div>

すべてのリフレッシャブルmaterialized viewの状態は、テーブル [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md) で確認できます。具体的には、リフレッシュの進行状況 (実行中の場合) 、前回および次回のリフレッシュ時刻、リフレッシュに失敗した場合の例外メッセージが含まれます。

リフレッシュを手動で停止、開始、トリガー、またはキャンセルするには、[`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views) を使用します。

リフレッシュの完了を待機するには、[`SYSTEM WAIT VIEW`](../system.md#wait-view) を使用します。特に、ビュー作成後の初回リフレッシュを待つ際に便利です。

:::note
豆知識: リフレッシュクエリでは、リフレッシュ対象のmaterialized view自体を読み取ることができ、その際にはリフレッシュ前のバージョンのデータが見えます。つまり、Conway&#39;s Game of Life を実装できます: https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

<div id="window-view">
  ## Window View
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
これは実験的な機能であり、将来のリリースで後方互換性のない変更が加えられる可能性があります。Window View と `WATCH` クエリの使用を有効にするには、[allow&#95;experimental&#95;window&#95;view](/ja/operations/settings/settings#allow_experimental_window_view) 設定を有効にしてください。`set allow_experimental_window_view = 1` コマンドを入力してください。
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

Window View は、time window ごとにデータを集約し、window が発火する準備が整うと結果を出力できます。レイテンシを低減するため、部分的な集約結果は内部 (または指定した) table に保存され、処理結果は指定した table に Push するか、WATCHクエリを使用して通知を Push できます。

Window View の作成は、`MATERIALIZED VIEW` の作成に似ています。Window View では、中間データを保存するための内部 table engine が必要です。内部ストレージは `INNER ENGINE` clause を使用して指定でき、Window View はデフォルトの内部 engine として `AggregatingMergeTree` を使用します。

`TO [db].[table]` を指定せずに Window View を作成する場合は、データ保存用の table engine である `ENGINE` を指定する必要があります。

<div id="time-window-functions">
  ### 時間ウィンドウ関数
</div>

[時間ウィンドウ関数](../../functions/time-window-functions.md)は、レコードが属するウィンドウの下限と上限を取得するために使用されます。Window View では、時間ウィンドウ関数を併用する必要があります。

<div id="time-attributes">
  ### 時間属性
</div>

Window View は、**処理時刻**と**イベント時刻**による処理をサポートします。

**処理時刻**では、ローカルマシンの時刻に基づいて Window View が結果を生成でき、既定で使用されます。これは最も単純な時間の考え方ですが、決定性はありません。処理時刻属性は、time window function の `time_attr` をテーブルのカラムに設定するか、関数 `now()` を使用することで定義できます。次のクエリは、処理時刻を持つ Window View を作成します。

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**イベント時刻** は、各イベントが生成元のデバイス上で実際に発生した時刻です。この時刻は通常、イベントの生成時にレコード内へ埋め込まれます。イベント時刻処理を使用すると、イベントの順序が前後している場合や到着が遅れたイベントがある場合でも、一貫した結果を得られます。Window View は `WATERMARK` 構文を使用してイベント時刻処理をサポートします。

Window View には 3 つのウォーターマーク戦略があります。

* `STRICTLY_ASCENDING`: これまでに観測されたタイムスタンプの最大値のウォーターマークを出力します。タイムスタンプがこの最大値より小さい行は遅延ではありません。
* `ASCENDING`: これまでに観測されたタイムスタンプの最大値から 1 を引いたウォーターマークを出力します。タイムスタンプがこの最大値以下の行は遅延ではありません。
* `BOUNDED`: WATERMARK=INTERVAL。観測されたタイムスタンプの最大値から指定した遅延を引いたウォーターマークを出力します。

以下のクエリは、`WATERMARK` を使用して Window View を作成する例です。

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

デフォルトでは、ウィンドウはウォーターマークに達すると発火し、ウォーターマークより遅れて到着した要素は破棄されます。Window View は、`ALLOWED_LATENESS=INTERVAL` を設定することで遅延イベント処理をサポートします。遅延の処理例は次のとおりです:

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

遅延発火によって出力された要素は、以前の計算結果が更新されたものとして扱う必要があることに注意してください。ウィンドウの終了時に発火するのではなく、Window View は遅延イベントが到着すると直ちに発火します。そのため、同じウィンドウに対して複数回出力されることになります。ユーザーは、これらの重複した結果を考慮するか、重複排除する必要があります。

`ALTER TABLE ... MODIFY QUERY` ステートメントを使用すると、Window View で指定した `SELECT` クエリを変更できます。新しい `SELECT` クエリの結果となるデータ構造は、`TO [db.]name` 句の有無にかかわらず、元の `SELECT` クエリと同じである必要があります。中間状態は再利用できないため、現在のウィンドウのデータは失われることに注意してください。

<div id="monitoring-new-windows">
  ### 新しいウィンドウの監視
</div>

Window View では、変更を監視するための [WATCH](../../../sql-reference/statements/watch.md) クエリをサポートしています。あるいは、`TO` 構文を使用して結果をテーブルに出力することもできます。

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

`LIMIT` を指定すると、クエリが終了するまでに受け取る更新の回数を設定できます。`EVENTS` 句を使用すると、クエリ結果の代わりに最新のクエリ watermark のみを取得する、`WATCH` クエリの簡易形式を利用できます。

<div id="settings-1">
  ### 設定
</div>

* `window_view_clean_interval`: 古いデータを解放するための Window View のクリーンアップ間隔 (秒) です。システム時刻または `WATERMARK` 設定に基づいて、まだ完全に発火していないウィンドウは保持され、それ以外のデータは削除されます。
* `window_view_heartbeat_interval`: watchクエリが動作中であることを示すハートビート間隔 (秒) です。
* `wait_for_window_view_fire_signal_timeout`: イベント時刻処理で Window View の発火シグナルを待機する際のタイムアウトです。

<div id="examples">
  ### 例
</div>

`data` という名前のログテーブルで、10秒ごとのクリックログ数をカウントする必要があるとします。テーブル構造は次のとおりです。

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

まず、10秒間隔のタンブルウィンドウを使用するWindow Viewを作成します：

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

次に、結果を取得するには `WATCH` クエリを使用します。

```sql
WATCH wv
```

ログがテーブル `data` に挿入されると、

```sql
INSERT INTO data VALUES(1,now())
```

`WATCH` クエリを実行すると、結果は次のように表示されます。

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

あるいは、`TO`構文を使用して、出力先を別のテーブルに指定することもできます。

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

追加の例は、ClickHouse のステートフルテスト (`*window_view*` という名前) にもあります。

<div id="window-view-usage">
  ### Window View の用途
</div>

Window View は、次のような場面で役立ちます。

* **監視**: ログのメトリクスを時間単位で集計・計算し、その結果をターゲットテーブルに出力します。ダッシュボードでは、そのターゲットテーブルをソーステーブルとして利用できます。
* **分析**: time window 内のデータを自動的に集計・前処理します。これは、大量のログを分析する際に役立ちます。前処理によって、複数のクエリで同じ計算を繰り返す必要がなくなり、クエリのレイテンシを低減できます。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseで時系列データを扱う](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* ブログ: [ClickHouseでオブザーバビリティ ソリューションを構築する - 第2部 - トレース](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

<div id="temporary-views">
  ## 一時ビュー
</div>

ClickHouse は、次の特性を持つ**一時ビュー**をサポートしています (該当する項目は一時テーブルと共通です) 。

* **セッション存続期間**
  一時ビューは現在のセッションの間のみ存在します。セッションが終了すると自動的に削除されます。

* **データベースなし**
  一時ビューをデータベース名で修飾することは**できません**。一時ビューはデータベースの外側 (セッションのネームスペース) に存在します。

* **レプリケーションなし / ON CLUSTER なし**
  一時オブジェクトはセッションローカルであり、`ON CLUSTER` を使用して作成することは**できません**。

* **名前解決**
  一時オブジェクト (テーブルまたはビュー) が永続オブジェクトと同じ名前を持ち、クエリがデータベース名を**付けずに**その名前を参照した場合は、**一時**オブジェクトが使用されます。

* **論理オブジェクト (ストレージなし)&#x20;**&#xA;一時ビューは `SELECT` テキストのみを保存します (内部的には `View` ストレージを使用します) 。データは永続化されず、`INSERT` も受け付けません。

* **Engine 句**
  `ENGINE` を指定する必要は**ありません**。`ENGINE = View` を指定した場合も無視され、同じ論理ビューとして扱われます。

* **セキュリティ / 権限**
  一時ビューの作成には `CREATE TEMPORARY VIEW` 権限が必要です。この権限は `CREATE VIEW` によって暗黙的に付与されます。

* **SHOW CREATE**
  一時ビューの DDL を表示するには、`SHOW CREATE TEMPORARY VIEW view_name;` を使用します。

<div id="temporary-views-syntax">
  ### 構文
</div>

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

`OR REPLACE` は一時ビューでは **サポートされていません** (一時テーブルとの整合性を保つためです) 。一時ビューを「置き換える」必要がある場合は、いったん削除してから再度作成してください。

<div id="examples">
  ### 例
</div>

一時テーブルを作成し、その上に一時ビューを作成します:

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

DDLを表示します:

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

削除するには:

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

<div id="temporary-views-limitations">
  ### 使用不可 / 制限事項
</div>

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **使用できません** (`DROP` + `CREATE` を使用してください) 。
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **使用できません**。
* `CREATE TEMPORARY VIEW db.view AS ...` → **使用できません** (データベース修飾子は使えません) 。
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **使用できません** (一時オブジェクトはセッションローカルです) 。
* `POPULATE`, `REFRESH`, `TO [db.table]`, inner engines, and all MV-specific clauses → 一時ビューには**適用されません**。

<div id="temporary-views-distributed-notes">
  ### 分散クエリに関する注意事項
</div>

**一時ビュー**は単なる定義にすぎないため、受け渡すデータはありません。一時ビューが**一時テーブル** (たとえば `Memory`) を参照している場合、そのデータは一時テーブルと同様に、分散クエリ実行時にリモートサーバーへ転送されることがあります。

<div id="temporary-views-distributed-example">
  #### 例
</div>

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```