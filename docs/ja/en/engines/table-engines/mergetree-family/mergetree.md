---
description: '`MergeTree`ファミリーのテーブルエンジンは、高いデータ取り込み率と膨大なデータ量に対応できるよう設計されています。'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'MergeTree テーブルエンジン'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # MergeTree テーブルエンジン
</div>

`MergeTree` エンジンおよび `MergeTree` ファミリーの他のエンジン (例: `ReplacingMergeTree`、`AggregatingMergeTree`) は、ClickHouse で最も一般的に使用されている、最も堅牢なテーブルエンジンです。

`MergeTree` ファミリーのテーブルエンジンは、高いデータ取り込み率と膨大なデータ量に対応できるよう設計されています。
挿入操作ではテーブルパーツが作成され、それらはバックグラウンドプロセスによって他のテーブルパーツとマージされます。

`MergeTree` ファミリーのテーブルエンジンの主な特長。

* テーブルの主キーは、各テーブルパーツ内のソート順を決定します (クラスタ化索引) 。また、主キーは個々の行ではなく、グラニュール と呼ばれる 8192 行のブロックを参照します。これにより、巨大なデータセットでも主キーを十分に小さく保ってメインメモリ上に保持しつつ、ディスク上のデータへ高速にアクセスできます。

* テーブルは任意のパーティション式を使ってパーティション化できます。パーティションプルーニングにより、クエリ条件で可能な場合は不要なパーティションの読み取りが省かれます。

* データは、高可用性、フェイルオーバー、無停止アップグレードのために、クラスター内の複数ノードにレプリケーションできます。[Data replication](/ja/engines/table-engines/mergetree-family/replication.md) を参照してください。

* `MergeTree` テーブルエンジンは、クエリ最適化に役立つさまざまな種類の統計情報とサンプリング手法をサポートしています。

:::note
名前は似ていますが、[Merge](/ja/engines/table-engines/special/merge) エンジンは `*MergeTree` エンジンとは異なります。
:::

<div id="table_engine-mergetree-creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

パラメータの詳細については、[CREATE TABLE](/ja/sql-reference/statements/create/table.md)ステートメントを参照してください

<div id="mergetree-query-clauses">
  ### クエリの句
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — エンジンの名前とパラメータです。`ENGINE = MergeTree()`. `MergeTree` エンジンにはパラメータはありません。

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` — ソートキーです。

カラム名のタプル、または任意の式です。例: `ORDER BY (CounterID + 1, EventDate)`。

主キーが定義されていない場合 (つまり `PRIMARY KEY` が指定されていない場合) 、ClickHouse はソートキーを主キーとして使用します。

ソートが不要な場合は、`ORDER BY tuple()` 構文を使用できます。
また、設定 `create_table_empty_primary_key_by_default` が有効になっている場合は、`CREATE TABLE` ステートメントに `ORDER BY ()` が暗黙的に追加されます。[主キーの選択](#selecting-a-primary-key)を参照してください。

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — [パーティションキー](/ja/engines/table-engines/mergetree-family/custom-partitioning-key.md)です。省略可能です。ほとんどの場合、パーティションキーは必要ありません。パーティション化が必要な場合でも、通常は月単位より細かいパーティションキーは不要です。パーティション化によってクエリが高速化されることはありません (`ORDER BY` 式とは対照的です) 。細かすぎるパーティション化は絶対に使用しないでください。データをクライアントの識別子や名前でパーティション化しないでください (代わりに、クライアントの識別子または名前を `ORDER BY` 式の最初のカラムにしてください) 。

月単位でパーティション化するには、`toYYYYMM(date_column)` 式を使用します。ここで `date_column` は、[Date](/ja/sql-reference/data-types/date.md) 型の日付を格納するカラムです。ここでのパーティション名は `"YYYYMM"` フォーマットです。

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — [ソートキーと異なる](#choosing-a-primary-key-that-differs-from-the-sorting-key)場合の主キーです。省略可能です。

ソートキー (`ORDER BY` 句を使用) を指定すると、暗黙的に主キーも指定されます。
通常は、ソートキーとは別に主キーを指定する必要はありません。

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — サンプリング式です。省略可能です。

指定する場合は、主キーに含まれている必要があります。
サンプリング式の結果は、符号なし整数である必要があります。

例: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`。

<div id="ttl">
  #### 有効期限 (TTL)
</div>

`TTL` — 行の保存期間と、[ディスクおよびボリューム間](#table_engine-mergetree-multiple-volumes)でのパーツの自動移動のロジックを指定するルールの一覧です。省略可能です。

式は `Date` または `DateTime` になる必要があります。例: `TTL date + INTERVAL 1 DAY`。

ルールの種類 `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` は、式の条件が満たされたとき (現在時刻に達したとき) に、そのパーツに対して実行するアクションを指定します。具体的には、期限切れの行の削除、パーツ内のすべての行で式の条件が満たされた場合に指定したディスク (`TO DISK 'xxx'`) またはボリューム (`TO VOLUME 'xxx'`) へパーツを移動すること、あるいは期限切れの行の値を集約することです。ルールのデフォルトの種類は削除 (`DELETE`) です。複数のルールを指定できますが、`DELETE` ルールは 1 つまでです。

詳細は、[カラムとテーブルの TTL](#table_engine-mergetree-ttl) を参照してください

<div id="settings">
  #### 設定
</div>

[MergeTree の設定](../../../operations/settings/merge-tree-settings.md)を参照してください。

**セクション設定の例**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

この例では、月単位でパーティション化するように設定しています。

また、ユーザー ID のハッシュをサンプリング式として設定しています。これにより、各 `CounterID` と `EventDate` ごとに、テーブル内のデータを疑似ランダム化できます。データの選択時に [SAMPLE](/ja/sql-reference/statements/select/sample) 句を指定すると、ClickHouse はユーザーのサブセットに対する均等な疑似ランダムサンプルを返します。

`index_granularity` 設定は省略できます。8192 がデフォルト値だからです。

<details markdown="1">
  <summary>テーブル作成の非推奨の方法</summary>

  :::note
  新しいプロジェクトではこの方法を使用しないでください。可能であれば、既存のプロジェクトも上記で説明した方法に切り替えてください。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **MergeTree() パラメーター**

  * `date-column` — [Date](/ja/sql-reference/data-types/date.md) 型のカラム名です。ClickHouse はこのカラムに基づいて、自動的に月単位のパーティションを作成します。パーティション名の形式は `"YYYYMM"` です。
  * `sampling_expression` — サンプリング式。
  * `(primary, key)` — 主キー。型: [Tuple()](/ja/sql-reference/data-types/tuple.md)
  * `index_granularity` — 索引の粒度です。索引の &quot;marks&quot; の間にあるデータの行数を表します。8192 という値は、ほとんどのタスクに適しています。

  **例**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  `MergeTree` エンジンの設定方法は、メインのエンジン設定方法として上記の例で示したものと同じです。
</details>

<div id="mergetree-data-storage">
  ## データストレージ
</div>

テーブルは、主キー順にソートされたデータパーツで構成されます。

データをテーブルに挿入すると、個別のデータパーツが作成され、それぞれが主キー順に辞書順ソートされます。たとえば、主キーが `(CounterID, Date)` の場合、パーツ内のデータは `CounterID` でソートされ、各 `CounterID` の中では `Date` の順に並びます。

異なるパーティションに属するデータは、別々のパーツに分けて格納されます。ClickHouse は、より効率的に保存するために、バックグラウンドでデータパーツをマージします。異なるパーティションに属するパーツがマージされることはありません。また、このマージの仕組みによって、同じ主キーを持つすべての行が同じデータパーツに入ることが保証されるわけではありません。

データパーツは `Wide` または `Compact` フォーマットで保存できます。`Wide` フォーマットでは各カラムがファイルシステム上の別々のファイルに保存され、`Compact` フォーマットではすべてのカラムが 1 つのファイルに保存されます。`Compact` フォーマットは、小さな挿入が頻繁に発生する場合のパフォーマンス向上に利用できます。

データの保存フォーマットは、テーブルエンジンの `min_bytes_for_wide_part` および `min_rows_for_wide_part` 設定によって制御されます。データパーツ内のバイト数または行数が対応する設定値より小さい場合、そのパーツは `Compact` フォーマットで保存されます。そうでない場合は `Wide` フォーマットで保存されます。これらの設定がどちらも指定されていない場合、データパーツは `Wide` フォーマットで保存されます。

各データパーツは、論理的にグラニュールへ分割されます。グラニュールは、ClickHouse がデータを選択する際に読み取る最小の不可分なデータセットです。ClickHouse は行や値を分割しないため、各グラニュールには常に整数個の行が含まれます。グラニュールの先頭行には、その行の主キーの値に対応するマークが付きます。ClickHouse は各データパーツについて、これらのマークを保存する索引ファイルを作成します。また、主キーに含まれるかどうかにかかわらず、各カラムについても同じマークを保存します。これらのマークにより、カラムファイル内のデータを直接見つけることができます。

グラニュールのサイズは、テーブルエンジンの `index_granularity` および `index_granularity_bytes` 設定によって制限されます。グラニュール内の行数は、行のサイズに応じて `[1, index_granularity]` の範囲になります。1 行のサイズが設定値より大きい場合、グラニュールのサイズは `index_granularity_bytes` を超えることがあります。この場合、グラニュールのサイズはその行のサイズと等しくなります。

<div id="primary-keys-and-indexes-in-queries">
  ## クエリにおける主キーと索引
</div>

`(CounterID, Date)` を主キーの例として考えると、この場合のソート順と索引は次のようになります。

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

データのクエリで次を指定した場合:

* `CounterID in ('a', 'h')` の場合、サーバーはマークの範囲 `[0, 3)` および `[6, 8)` のデータを読み取ります。
* `CounterID IN ('a', 'h') AND Date = 3` の場合、サーバーはマークの範囲 `[1, 3)` および `[7, 8)` のデータを読み取ります。
* `Date = 3` の場合、サーバーはマークの範囲 `[1, 10]` のデータを読み取ります。

上記の例から、フルスキャンよりも索引を使用する方が常に効果的であることがわかります。

スパースインデックスでは、余分なデータが読み取られることがあります。主キーの単一の範囲を読み取る場合、各データブロックで最大 `index_granularity * 2` 行の余分な行が読み取られる可能性があります。

スパースインデックスを使用すると、非常に多くのテーブル行を扱えます。これは、ほとんどの場合、このような索引がコンピューターの RAM に収まるためです。

ClickHouse では、一意な主キーは必須ではありません。同じ主キーを持つ複数の行を挿入できます。

`PRIMARY KEY` および `ORDER BY` 句では `Nullable` 型の式を使用できますが、これは強く非推奨です。この機能を有効にするには、[allow&#95;nullable&#95;key](/ja/operations/settings/merge-tree-settings/#allow_nullable_key) 設定を有効にします。`ORDER BY` 句における `NULL` 値には、[NULLS&#95;LAST](/ja/sql-reference/statements/select/order-by.md/#sorting-of-special-values) の原則が適用されます。

<div id="selecting-a-primary-key">
  ### 主キーの選択
</div>

主キーのカラム数に明示的な制限はありません。データ構造に応じて、主キーに含めるカラム数は増減できます。これにより、次のような効果が得られます。

* 索引の性能向上。

  主キーが `(a, b)` の場合、次の条件を満たしていれば、さらにカラム `c` を追加することで性能が向上します。

  * カラム `c` に条件を指定するクエリがある。
  * `(a, b)` の値が同一の長いデータ範囲 (`index_granularity` の数倍以上) がよく存在する。つまり、カラムを追加することで、かなり長いデータ範囲をスキップできる場合です。

* データ圧縮の向上。

  ClickHouse は主キーでデータをソートするため、一貫性が高いほど圧縮効率も向上します。

* [CollapsingMergeTree](/ja/engines/table-engines/mergetree-family/collapsingmergetree) および [SummingMergeTree](/ja/engines/table-engines/mergetree-family/summingmergetree.md) エンジンでデータパーツをマージする際に、追加のロジックを適用できる。

  この場合は、主キーとは異なる *ソートキー* を指定するのが合理的です。

主キーが長すぎると、insert の性能とメモリ消費に悪影響があります。ただし、主キーに追加したカラムは、`SELECT` クエリ実行時の ClickHouse の性能には影響しません。

`ORDER BY tuple()` 構文を使うと、主キーなしでテーブルを作成できます。この場合、ClickHouse はデータを挿入順に格納します。`INSERT ... SELECT` クエリでデータを挿入する際にこの順序を保持したい場合は、[max&#95;insert&#95;threads = 1](/ja/operations/settings/settings#max_insert_threads) を設定してください。

元の順序でデータを取得するには、[シングルスレッド](/ja/operations/settings/settings.md/#max_threads) の `SELECT` クエリを使用します。

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### ソートキーと異なる主キーを選択する
</div>

ソートキー (データパーツ内の行をソートするための式) とは異なる主キー (各 mark ごとに索引ファイルへ書き込まれる値の式) を指定できます。この場合、主キー式のタプルはソートキー式のタプルのプレフィックスでなければなりません。

この機能は、[SummingMergeTree](/ja/engines/table-engines/mergetree-family/summingmergetree.md) および
[AggregatingMergeTree](/ja/engines/table-engines/mergetree-family/aggregatingmergetree.md) テーブルエンジンを使用する場合に役立ちます。これらのエンジンを使用する一般的なケースでは、テーブルには *次元* と *指標* という 2 種類のカラムがあります。典型的なクエリでは、任意の `GROUP BY` と次元によるフィルタリングを使って、指標カラムの値を集約します。SummingMergeTree と AggregatingMergeTree は、ソートキーの値が同じ行を集約するため、すべての次元をそこに追加するのが自然です。その結果、キー式は多数のカラムからなる長い一覧となり、新しい次元が追加されるたびにこの一覧を頻繁に更新しなければなりません。

このような場合は、効率的な範囲スキャンに必要な少数のカラムだけを主キーに残し、残りの次元カラムをソートキーのタプルに追加するのが合理的です。

ソートキーの [ALTER](/ja/sql-reference/statements/alter/index.md) は軽量な操作です。新しいカラムをテーブルとソートキーに同時に追加しても、既存のデータパーツを変更する必要がないためです。古いソートキーは新しいソートキーのプレフィックスであり、さらに新しく追加されたカラムにはデータが存在しないため、テーブル変更時点ではデータは古いソートキーと新しいソートキーの両方に対してソート済みになっています。

<div id="use-of-indexes-and-partitions-in-queries">
  ### クエリにおける索引とパーティションの使用
</div>

`SELECT` クエリでは、ClickHouse は索引を使用できるかどうかを判定します。索引を使用できるのは、`WHERE/PREWHERE` 句に、等値比較または不等値比較を表す式が (AND 条件の一部として、または式全体として) 含まれている場合、または主キーやパーティションキーに含まれるカラムや式、それらのカラムに対する一部の部分反復関数、あるいはそれらの式の論理関係に対して、固定プレフィックス付きの `IN` または `LIKE` が含まれている場合です。

そのため、主キーの 1 つまたは複数の範囲に対するクエリを高速に実行できます。この例では、特定のトラッキングタグに対する場合、特定のタグと日付範囲に対する場合、特定のタグと日付に対する場合、複数のタグと日付範囲に対する場合などに、クエリを高速に実行できます。

次のように設定されたエンジンを見てみましょう。

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

この場合、クエリでは次のようになります:

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse は、主キーの索引を使用して不要なデータを絞り込み、月単位のパーティション化キーを使用して、対象の日付範囲外のパーティションを絞り込みます。

上記のクエリから、複雑な式に対しても索引が使用されることがわかります。テーブルからの読み取りは、索引を使用してもフルスキャンより遅くならないように構成されています。

以下の例では、索引を使用できません。

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

クエリの実行時に ClickHouse が索引を使用できるかどうかを確認するには、設定 [force&#95;index&#95;by&#95;date](/ja/operations/settings/settings.md/#force_index_by_date) と [force&#95;primary&#95;key](/ja/operations/settings/settings#force_primary_key) を使用します。

月単位でパーティション化するためのキーを使用すると、該当する範囲の日付を含むデータブロックだけを読み取れます。この場合、データブロックには複数の日付のデータ (最大で 1 か月分全体) が含まれることがあります。ブロック内のデータは主キーでソートされますが、主キーの先頭カラムが日付とは限りません。そのため、主キーのプレフィックスを指定せずに日付条件だけを含むクエリを使用すると、単一の日付を対象とする場合よりも多くのデータが読み取られます。

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### 主キー内の決定論的な式に対する索引の利用
</div>

主キーには、カラム名だけでなく式も含めることができます。これらの式は単純な関数の連鎖に限られず、決定論的である限り、任意の式ツリー (たとえば、入れ子になった関数や複合式) にできます。

ある式が**決定論的**であるとは、同じ入力値に対して常に同じ結果を返すことを意味します (例: `length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`。`now()` や `rand()` とは異なります) 。主キーに決定論的な式が含まれている場合、ClickHouse はそれらをクエリ内の定数値に適用し、その結果を使って主キー索引上の条件を構築できます。これにより、`=`, `IN`, `has` のような述語でデータスキッピングが可能になります。

一般的なユースケースとしては、主キーをコンパクトに保ちつつ (たとえば長い `String` の代わりにハッシュを格納する) 、元のカラムに対する述語でも索引を利用できるようにすることが挙げられます。

決定論的な (ただし単射ではない) 主キーの例:

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

索引を利用できる述語の例:

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

これらのケースでは、ClickHouse は `length('alice')` (およびその他の定数) を一度だけ計算し、その長さの値を使って主キー索引内の範囲を絞り込みます。文字列の長さは **単射ではない** ため、異なる `user_id` 文字列が同じ長さになることがあり、その結果、索引が余分な グラニュール (false positives) を読み取る可能性があります。元の述語 (`user_id = ...`、`IN` など) は読み取り後にも適用されるため、結果の正しさは保たれます。

決定論的な式がさらに **単射** でもある場合 (使用される引数の型において、異なる入力が同じ出力にならない場合) 、ClickHouse は否定形でも索引を効果的に使用できます: `!=`、`NOT IN`、`NOT has(...)`。たとえば、`reverse(p)` と `hex(p)` は `String` に対して単射です。

単射な主キーの例:

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

より複雑な単射な式もサポートされています。たとえば次のとおりです。

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

索引を利用できる述語の例:

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### 部分単調な主キーに対する索引の使用
</div>

たとえば、月の日付について考えてみましょう。日付は 1 か月の間では[単調数列](https://en.wikipedia.org/wiki/Monotonic_function)を成しますが、より長い期間では単調ではありません。これは部分単調な数列です。ユーザーが部分単調な主キーで table を作成すると、ClickHouse は通常どおりスパースインデックスを作成します。ユーザーがこの種の table からデータを select すると、ClickHouse はクエリ条件を解析します。ユーザーが索引の 2 つのマークの間にあるデータを取得しようとしており、その 2 つのマークがどちらも同じ月の範囲内にある場合、ClickHouse はこのケースでは索引を使用できます。これは、クエリのパラメータと索引マークの間の距離を計算できるためです。

クエリパラメータの範囲内にある主キーの値が単調な数列を表していない場合、ClickHouse は索引を使用できません。この場合、ClickHouse はフルスキャン方式を使用します。

ClickHouse はこのロジックを月の日付の数列だけでなく、部分単調な数列を表すすべての主キーに対して使用します。

<div id="table_engine-mergetree-data_skipping-indexes">
  ### データスキッピング索引
</div>

索引の宣言は、`CREATE`クエリのカラム定義部分にあります。

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

`*MergeTree` ファミリーのテーブルでは、データスキッピング索引を指定できます。

これらの索引は、`granularity_value` 個の granule で構成される block について、指定した expression に関する一部の情報を集約します (granule のサイズは、テーブルエンジンの `index_granularity` 設定で指定します) 。その後、これらの集約情報は `SELECT` クエリで使用され、`where` 条件を満たさない大きな data block をスキップすることで、ディスクから読み取るデータ量を削減します。

`GRANULARITY` 句は省略でき、`granularity_value` のデフォルト値は 1 です。

**例**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

この例の索引を使用すると、ClickHouse は次のクエリでディスクから読み取るデータ量を削減できます:

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

データスキッピング索引は、複合カラムにも作成できます。

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### スキップ索引タイプ
</div>

`MergeTree` テーブルエンジンは、以下の種類のスキップ索引をサポートしています。
スキップ索引をパフォーマンス最適化にどのように活用できるかについて詳しくは、
[&quot;ClickHouseのデータスキッピングインデックスについて&quot;](/ja/optimize/skipping-indexes) を参照してください。

* [`MinMax`](#minmax) 索引
* [`Set`](#set) 索引
* [`bloom_filter`](#bloom-filter) 索引
* [`ngrambf_v1`](#n-gram-bloom-filter) 索引 *(非推奨)*
* [`tokenbf_v1`](#token-bloom-filter) 索引 *(非推奨)*
* [`text`](#text) 索引
* [`vector_similarity`](#vector-similarity) 索引

<div id="minmax">
  #### MinMax スキップ索引
</div>

各インデックスグラニュールには、式の最小値と最大値が格納されます。
(式の型が `tuple` の場合は、各タプル要素ごとの最小値と最大値が格納されます。)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

各インデックスグラニュールには、指定した式の一意な値を最大 `max_rows` 個まで格納できます。
`max_rows = 0` は「一意な値をすべて格納する」ことを意味します。

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### ブルームフィルタ
</div>

各インデックスグラニュールに、指定したカラム用の [ブルームフィルタ](https://en.wikipedia.org/wiki/Bloom_filter) を格納します。

```text title="Syntax"
bloom_filter([false_positive_rate])
```

`false_positive_rate` パラメータには 0 から 1 までの値を指定でき (デフォルト: `0.025`) 、陽性判定が生成される確率を指定します (これにより、読み取るデータ量が増加します) 。

以下のデータ型がサポートされています。

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note Map データ型: キーまたは値を指定した索引の作成
`Map` データ型では、[`mapKeys`](/ja/sql-reference/functions/tuple-map-functions.md/#mapKeys) または [`mapValues`](/ja/sql-reference/functions/tuple-map-functions.md/#mapValues) 関数を使用して、キーに対して索引を作成するか、値に対して索引を作成するかをクライアントが指定できます。
:::

:::note JSON data type: JSON パスの索引作成
[`JSON`](/ja/sql-reference/data-types/newjson) データ型では、[`JSONAllPaths`](/ja/sql-reference/functions/json-functions#JSONAllPaths) 関数を使用して、パスの集合に対する bloom filter 索引を作成できます。これにより、クエリ対象の JSON パスが存在しないグラニュールをスキップできます。詳細は [JSON のデータスキッピングインデックス](/ja/sql-reference/data-types/newjson#data-skipping-indexes-for-json) を参照してください。
:::

<div id="n-gram-bloom-filter">
  #### N-gram ブルームフィルタ *(非推奨)*
</div>

:::note
ClickHouse バージョン 26.2 以降では `text` 索引が一般提供 (GA) となったため、`ngrambf_v1` 索引は全文検索には推奨されなくなりました。

詳しくは、[「テキスト索引を使った全文検索」](./textindexes.md) を参照してください。
:::

各インデックスグラニュールには、指定したカラムの [N-gram](https://en.wikipedia.org/wiki/N-gram) に対する [ブルームフィルタ](https://en.wikipedia.org/wiki/Bloom_filter) が格納されます。

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| パラメータ                           | 説明                                                                         |
| ------------------------------- | -------------------------------------------------------------------------- |
| `n`                             | ngram のサイズ                                                                 |
| `size_of_bloom_filter_in_bytes` | ブルームフィルタのサイズ (バイト単位) 。ここでは `256` や `512` などの大きな値を使用できます。十分に圧縮できるためです。 |
| `number_of_hash_functions`      | ブルームフィルタで使用する hash function の数。                                       |
| `random_seed`                   | ブルームフィルタの hash function 用の seed。                                      |

この索引は、次のデータ型でのみ使用できます。

* [`String`](/ja/sql-reference/data-types/string.md)
* [`FixedString`](/ja/sql-reference/data-types/fixedstring.md)
* [`Map`](/ja/sql-reference/data-types/map.md)

`ngrambf_v1` のパラメータを見積もるには、次の[ユーザー定義関数 (UDF) ](/ja/sql-reference/statements/create/function.md)を使用できます。

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

これらの関数を使用するには、少なくとも 2 つのパラメータを指定する必要があります。

* `total_number_of_all_grams`
* `probability_of_false_positives`

たとえば、グラニュール内に `4300` 個の N-gram があり、偽陽性の発生確率を `0.0001` 未満に抑えたいとします。
その場合、他のパラメータは次のクエリを実行して推定できます。

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

もちろん、これらの関数を使って、ほかの条件に対するパラメータを見積もることもできます。
上記の関数は、[こちら](https://hur.st/bloomfilter)の ブルームフィルタ計算機を参考にしています。

<div id="token-bloom-filter">
  #### トークン ブルームフィルタ
</div>

:::note
ClickHouse バージョン 26.2 で `text` 索引が一般提供 (GA) となったことにより、`tokenbf_v1` 索引は全文検索向けには推奨されなくなりました。

詳細は、[&quot;テキスト索引による全文検索&quot;](./textindexes.md) のページを参照してください。
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### スパースグラムのブルームフィルタ
</div>

スパースグラムのブルームフィルタは `ngrambf_v1` と似ていますが、N-gram の代わりに [スパースグラムトークン](/ja/sql-reference/functions/string-functions.md/#sparseGrams) を使用します。

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### テキスト索引
</div>

トークン化された文字列データに対して転置索引を構築し、効率的で決定論的な全文検索を可能にします。詳細は[こちら](textindexes.md)を参照してください。

<div id="vector-similarity">
  #### ベクトル類似度
</div>

近似最近傍探索をサポートしています。詳細については[こちら](annindexes.md)を参照してください。

<div id="functions-support">
  ### 関数のサポート
</div>

`WHERE` 句の条件には、カラムに対して動作する関数の呼び出しが含まれる場合があります。カラムが索引の一部である場合、ClickHouse はその関数の実行時にこの索引を使用しようとします。ClickHouse では、索引の利用に対応する関数のサブセットが複数サポートされています。

`set` 型の索引は、すべての関数で利用できます。その他の索引タイプでサポートされる関数は、次のとおりです。

| 関数 (演算子)  / 索引                                                                                                            | 主キー | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | text |
| ------------------------------------------------------------------------------------------------------------------------- | --- | ------ | -------------- | -------------- | ---------------- | ---------------- | ---- |
| [equals (=, ==)](/ja/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔   | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notEquals(!=, &lt;&gt;)](/ja/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔   | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [like](/ja/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔   | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [notLike](/ja/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔   | ✔      | ✔              | ✔              | ✗                | ✔                | ✗    |
| [match](/ja/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗   | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [startsWith](/ja/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔   | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [endsWith](/ja/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗   | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [multiSearchAny](/ja/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗   | ✗      | ✔              | ✗              | ✗                | ✗                | ✔    |
| [multiSearchAnyUTF8](/ja/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [multiMatchAny](/ja/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [in](/ja/sql-reference/functions/in-functions)                                                                               | ✔   | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notIn](/ja/sql-reference/functions/in-functions)                                                                            | ✔   | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [less (`<`)](/ja/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔   | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greater (`>`)](/ja/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔   | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [lessOrEquals (`<=`)](/ja/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔   | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greaterOrEquals (`>=`)](/ja/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔   | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [empty](/ja/sql-reference/functions/array-functions/#empty)                                                                  | ✔   | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [notEmpty](/ja/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗   | ✔      | ✗              | ✗              | ✗                | ✔                | ✗    |
| [has](/ja/sql-reference/functions/array-functions#has)                                                                       | ✔   | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [hasAny](/ja/sql-reference/functions/array-functions#hasAny)                                                                 | ✗   | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasAll](/ja/sql-reference/functions/array-functions#hasAll)                                                                 | ✗   | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasToken](/ja/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗   | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenOrNull](/ja/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗   | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenCaseInsensitive (`*`)](/ja/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗   | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasTokenCaseInsensitiveOrNull (`*`)](/ja/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗   | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasAnyTokens](/ja/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [hasAllTokens](/ja/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [pointInPolygon](/ja/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔   | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [mapContains (mapContainsKey)](/ja/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsKeyLike](/ja/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValue](/ja/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValueLike](/ja/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗   | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |

定数の引数が ngram サイズより小さい関数は、`ngrambf_v1` によるクエリ最適化には使用できません。

(*) `hasTokenCaseInsensitive` と `hasTokenCaseInsensitiveOrNull` を有効に機能させるには、`tokenbf_v1` 索引を小文字化したデータに対して作成する必要があります。たとえば、`INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)` のようにします。

:::note
ブルームフィルタでは偽陽性が発生する可能性があるため、`ngrambf_v1`、`tokenbf_v1`、`sparse_grams`、`bloom_filter` の各索引は、関数の結果が `false` になることが想定されるクエリの最適化には使用できません。

たとえば:

* 最適化可能:
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* 最適化不可:
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## PROJECTION
</div>

PROJECTIONは [materialized view](/ja/sql-reference/statements/create/view) に似ていますが、パートレベルで定義されます。整合性が保証され、クエリでも自動的に使用されます。

:::note
PROJECTIONを実装する際は、[force&#95;optimize&#95;projection](/ja/operations/settings/settings#force_optimize_projection) 設定も考慮してください。
:::

PROJECTIONは、[FINAL](/ja/sql-reference/statements/select/from#final-modifier) modifier を含む `SELECT` ステートメントではサポートされていません。

<div id="projection-query">
  ### PROJECTIONクエリ
</div>

PROJECTIONクエリは、PROJECTIONを定義するクエリです。親テーブルからデータを暗黙的に選択します。
**構文**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

PROJECTIONは、[ALTER](/ja/sql-reference/statements/alter/projection.md)ステートメントを使用して変更または削除できます。

<div id="projection-index">
  ### PROJECTION 索引
</div>

PROJECTION 索引は、PROJECTION サブシステムを拡張し、PROJECTION レベルの索引を軽量かつ明示的に定義できるようにします。
外部的には、PROJECTION 索引も引き続き PROJECTION ですが、構文がより簡潔で、意図もより明確です。つまり、マテリアライズされたデータを提供するのではなく、フィルタリング専用の式を定義します。
内部的には、PROJECTION 索引は通常の PROJECTION のように、元のテーブルを並べ替えた行順でマテリアライズしません。
代わりに、その並べ替えは数値の permutation カラム `_part_offset` として保存されます。つまり、`SELECT _part_offset ORDER BY <index_expr>` です。

<div id="projection-index-syntax">
  #### 構文
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

例:

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### 索引タイプ
</div>

現在サポートされているのは次のとおりです。

* **basic**: 式に対する通常の MergeTree 索引と同等です。

このフレームワークでは、今後さらに多くの索引タイプを追加できるようになっています。

<div id="projection-storage">
  ### プロジェクションのストレージ
</div>

プロジェクションはパートディレクトリ内に保存されます。索引に似ていますが、無名の `MergeTree` テーブルのパートを格納するサブディレクトリを含みます。このテーブルは、プロジェクションの定義クエリによって決まります。`GROUP BY` clause がある場合、基盤となるストレージエンジンは [AggregatingMergeTree](aggregatingmergetree.md) となり、すべての aggregate functions は `AggregateFunction` に変換されます。`ORDER BY` clause がある場合、`MergeTree` テーブルはそれを主キー式として使用します。merge process の際、projection part はそのストレージのマージルーチンによってマージされます。親テーブルのパートの checksum は、プロジェクションのパートの checksum と結合されます。その他のメンテナンス jobs は、スキップ索引と同様です。

<div id="projection-query-analysis">
  ### クエリ分析
</div>

1. 指定されたクエリに対してそのプロジェクションを使用できるか、つまり基となるテーブルにクエリした場合と同じ結果を返せるかを確認します。
2. 読み取るグラニュール数が最も少ない、使用可能な最適な候補を選択します。
3. プロジェクションを使用するクエリパイプラインは、元のパーツを使用するものとは異なります。一部のパーツにプロジェクションが存在しない場合は、その場でそれを「投影」するパイプラインを追加できます。

<div id="concurrent-data-access">
  ## テーブルへの同時アクセス
</div>

テーブルへの同時アクセスには、マルチバージョニングを使用しています。つまり、テーブルに対して読み取りと更新が同時に行われている場合でも、データはクエリ実行時点で有効なパーツの集合から読み取られます。長時間保持されるロックはありません。挿入処理が読み取り操作の妨げになることもありません。

テーブルからの読み取りは自動的に並列化されます。

<div id="table_engine-mergetree-ttl">
  ## カラムとテーブルの有効期限 (TTL)
</div>

値の有効期間を決定します。

`TTL` 句は、テーブル全体と個々のカラムに設定できます。テーブルレベルの `TTL` では、ディスクやボリューム間でのデータの自動移動や、すべてのデータの有効期限が切れたパーツの再圧縮のロジックも指定できます。

式の評価結果は、[Date](/ja/sql-reference/data-types/date.md)、[Date32](/ja/sql-reference/data-types/date32.md)、[DateTime](/ja/sql-reference/data-types/datetime.md)、または [DateTime64](/ja/sql-reference/data-types/datetime64.md) データ型である必要があります。

:::tip[有効期限 (TTL)式では非決定論的関数を避けてください]
有効期限 (TTL) はバックグラウンドマージ時に評価され、挿入時には評価されません。
`rand()`, `now()`, `now64()` のような関数は、マージのたびに再評価されるため、削除の挙動が予測不能になります。
ClickHouse はカラムにまったく依存しない式をブロックしますが、現在のところ、カラム参照に非決定論的関数が混在している場合 (例: `ts + rand()`) は拒否しません。予測可能な結果にするため、有効期限 (TTL)式は決定論的で、カラムに由来する値のみに基づくべきです。
:::

**構文**

カラムの有効期限 (TTL)を設定する場合:

```sql
TTL time_column
TTL time_column + interval
```

`interval` を定義するには、[時間間隔](/ja/sql-reference/operators#operators-for-working-with-dates-and-times)演算子を使用します。たとえば:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### カラムの有効期限 (TTL)
</div>

カラム内の値の有効期限が切れると、ClickHouse はそれらをそのカラムのデータ型のデフォルト値に置き換えます。データパート内のそのカラムの値がすべて期限切れになると、ClickHouse はファイルシステム上のそのデータパートからそのカラムを削除します。

`TTL` 句はキーカラムには使用できません。

**例**

<div id="creating-a-table-with-ttl">
  #### `TTL` を設定したテーブルの作成:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### 既存のテーブルのカラムに有効期限 (TTL) を追加する
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### カラムの有効期限 (TTL) の変更
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### テーブルの有効期限 (TTL)
</div>

テーブルには、有効期限が切れた行を削除するための式と、[ディスクまたはボリューム](#table_engine-mergetree-multiple-volumes)間でパーツを自動的に移動するための複数の式を設定できます。テーブル内の行が有効期限切れになると、ClickHouse は該当する行をすべて削除します。パーツの移動または再圧縮では、パーツ内のすべての行が `TTL` 式の条件を満たしている必要があります。

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

各有効期限 (TTL)式の後ろには、有効期限 (TTL)ルールの種類を指定できます。これは、式の条件が満たされた時点 (現在時刻に達した時点) で実行されるアクションに影響します。

* `DELETE` - 期限切れの行を削除します (デフォルトのアクション) 。
* `RECOMPRESS codec_name` - `codec_name` でデータパートを再圧縮します。
* `TO DISK 'aaa'` - パートをディスク `aaa` に移動します。
* `TO VOLUME 'bbb'` - パートをボリューム `bbb` に移動します。
* `GROUP BY` - 期限切れの行を集約します。

`DELETE` アクションは `WHERE` 句と組み合わせて使用でき、フィルタリング条件に基づいて、期限切れの行の一部だけを削除できます。

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY` 式は、テーブルの主キーのプレフィックスである必要があります。

あるカラムが `GROUP BY` 式に含まれておらず、`SET` 句でも明示的に設定されていない場合、結果の行にはグループ化された複数の行のうちいずれかの値が入ります (そのカラムに集約関数 `any` が適用された場合と同様です) 。

**例**

<div id="creating-a-table-with-ttl">
  #### `TTL` を設定したテーブルの作成:
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### テーブルの`有効期限 (TTL)`を変更する:
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

1 か月後に有効期限が切れる行を持つテーブルを作成します。日付が月曜日に当たる期限切れの行は削除されます:

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### 期限切れになった行が再圧縮されるテーブルの作成:
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

期限切れの行が集計されるテーブルを作成します。結果の行では、`x` にはグループ化された行全体の最大値が、`y` には最小値が、`d` にはグループ化された行のいずれかの値が入ります。

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### 期限切れデータの削除
</div>

`有効期限 (TTL)` が期限切れになったデータは、ClickHouse がデータパーツをマージする際に削除されます。

ClickHouse はデータの期限切れを検出すると、予定外のマージを実行します。このようなマージの頻度は、`merge_with_ttl_timeout` で制御できます。値が低すぎると、予定外のマージが多数実行され、多くのリソースを消費する可能性があります。

マージの合間に `SELECT` クエリを実行すると、期限切れのデータが返されることがあります。これを避けるには、`SELECT` の前に [OPTIMIZE](/ja/sql-reference/statements/optimize.md) クエリを使用してください。

**関連項目**

* [ttl&#95;only&#95;drop&#95;parts](/ja/operations/settings/merge-tree-settings#ttl_only_drop_parts) 設定

<div id="disk-types">
  ## ディスクタイプ
</div>

ローカルのブロックデバイスに加えて、ClickHouse は次のストレージタイプをサポートしています。

* [`s3` for S3 and MinIO](#table_engine-mergetree-s3)
* [`gcs` for GCS](/ja/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [`blob_storage_disk` for Azure Blob Storage](/ja/operations/storing-data#azure-blob-storage)
* [`hdfs` for HDFS](/ja/engines/table-engines/integrations/hdfs)
* [`web` for Webからの読み取り専用](/ja/operations/storing-data#web-storage)
* [`cache` for ローカルキャッシュ](/ja/operations/storing-data#using-local-cache)
* [`s3_plain` for S3 へのバックアップ](/ja/operations/backup/disk)
* [`s3_plain_rewritable` for S3 内の不変の非レプリケートテーブル](/ja/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## データの保存に複数のブロックデバイスを使用する
</div>

<div id="introduction">
  ### はじめに
</div>

`MergeTree` family のテーブルエンジンでは、複数のブロックデバイスにデータを保存できます。たとえば、あるテーブルのデータが実質的に「ホット」と「コールド」に分かれるような場合に便利です。最新のデータは頻繁に参照されますが、必要な容量はわずかです。一方で、蓄積された履歴データは参照される頻度が低くなります。複数のディスクを利用できる場合は、「ホット」データを高速なディスク (たとえば NVMe SSD やメモリ上) に、「コールド」データを比較的低速なディスク (たとえば HDD) に配置できます。

これは、S3 やその他のオブジェクトストレージディスクを含む、すべてのディスクタイプに当てはまります。たとえば、単一のボリューム内で複数の S3 バケットにデータを分散したり、ローカルディスクから S3 へデータを移動する階層型ポリシーを作成したりできます。詳しくは、[複数のボリュームで S3 ディスクを使用する](#s3-multiple-volumes)を参照してください。

データパートは、`MergeTree` エンジンのテーブルにおける移動可能な最小単位です。1 つのパートに属するデータは 1 つのディスクに保存されます。データパートは、バックグラウンドで (ユーザー設定に従って) ディスク間を移動できるほか、[ALTER](/ja/sql-reference/statements/alter/partition) クエリを使用して移動することもできます。

<div id="terms">
  ### 用語
</div>

* ディスク — ファイルシステムにマウントされたブロックデバイス。
* デフォルトディスク — [path](/ja/operations/server-configuration-parameters/settings.md/#path) サーバー設定で指定されたパスに対応するディスク。
* ボリューム — 同等のディスクを順序付けた集合 ([JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) に類似) 。
* ストレージポリシー — ボリュームの集合と、それらの間でデータを移動するためのルール。

ここで説明したエンティティの名前は、システムテーブル [system.storage&#95;policies](/ja/operations/system-tables/storage_policies) と [system.disks](/ja/operations/system-tables/disks) で確認できます。テーブルに設定済みのストレージポリシーのいずれかを適用するには、`MergeTree` family のテーブルで `storage_policy` 設定を使用します。

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### 設定
</div>

ディスク、ボリューム、ストレージポリシーは、`config.d` ディレクトリ内のファイル、または `<storage_configuration>` タグ内で宣言する必要があります。

:::tip
ディスクはクエリの `SETTINGS` セクションで宣言することもできます。これは、たとえば URL 上でホストされているディスクを一時的にアタッチして、アドホックな分析を行う場合に便利です。
詳しくは [dynamic storage](/ja/operations/storing-data#dynamic-configuration) を参照してください。
:::

設定の構造:

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

タグ:

* `<disk_name_N>` — ディスク名。名前はすべてのディスクで異なっている必要があります。
* `path` — サーバーがデータ (`data` および `shadow` フォルダー) を保存するパス。末尾は &#39;/&#39; にする必要があります。
* `keep_free_space_bytes` — 確保しておく空きディスク容量。

ディスク定義の順序は重要ではありません。

ストレージポリシー設定のマークアップ:

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

タグ:

* `policy_name_N` — ポリシー名。ポリシー名は一意である必要があります。
* `volume_name_N` — ボリューム名。ボリューム名は一意である必要があります。
* `disk` — ボリューム内のディスク。
* `max_data_part_size_bytes` — ボリューム内のいずれかのディスクに格納できるパーツの最大サイズ。マージ後のパーツの推定サイズが `max_data_part_size_bytes` を超える場合、そのパーツは次のボリュームに書き込まれます。基本的には、この機能により、新しい/小さいパーツはホット (SSD) ボリュームに保持し、サイズが大きくなったらコールド (HDD) ボリュームへ移動できます。ポリシーにボリュームが 1 つしかない場合は、この設定を使用しないでください。
* `move_factor` — 利用可能な空き容量がこの係数を下回ると、データは自動的に次のボリューム (存在する場合) へ移動し始めます (デフォルトは 0.1) 。ClickHouse は既存のパーツをサイズの大きい順 (降順) にソートし、`move_factor` の条件を満たすのに十分な合計サイズとなるパーツを選択します。すべてのパーツの合計サイズでも不十分な場合は、すべてのパーツが移動されます。
* `perform_ttl_move_on_insert` — データパーツ INSERT 時の TTL move を無効にします。デフォルトでは (有効な場合) 、TTL move ルールですでに期限切れになっているデータパーツを insert すると、そのパーツは直ちに move ルールで指定されたボリューム/ディスクに送られます。宛先のボリューム/ディスクが低速な場合 (たとえば S3) 、これにより insert が大幅に遅くなることがあります。無効にした場合、すでに期限切れのデータパーツはデフォルトのボリュームに書き込まれ、その直後に TTL ボリュームへ移動されます。
* `load_balancing` - ディスクの負荷分散ポリシー。`round_robin` または `least_used`。
* `least_used_ttl_ms` - すべてのディスクの利用可能な空き容量を更新するための timeout (Milliseconds 単位) を設定します (`0` - 常に更新、`-1` - 更新しない、デフォルトは `60000`) 。なお、ディスクが ClickHouse 専用で使用され、オンラインでの filesystem の拡張/縮小の対象にならない場合は `-1` を使用できます。それ以外の場合は、最終的に不正確な容量配分につながるため、推奨されません。
* `prefer_not_to_merge` — この設定は使用しないでください。このボリューム上でのデータパーツのマージを無効にします (有害であり、パフォーマンス低下を招きます) 。この設定を有効にすると (しないでください) 、このボリューム上でのデータのマージは許可されません (望ましくありません) 。これにより ClickHouse の低速ディスクの扱いを制御できますが (その必要はありません。何かを制御したいと思ったなら、それは誤りです) 、ClickHouse のほうが適切に判断するため、この設定は使用しないでください。
* `volume_priority` — ボリュームが埋められる優先順位 (順序) を定義します。値が小さいほど優先度が高くなります。パラメータ値は natural numbers で、数字を飛ばさずに 1 から N (最低優先度に対応する値) までの範囲全体を満たす必要があります。
  * *すべての*ボリュームにタグが付いている場合は、指定された順序で優先されます。
  * *一部の*ボリュームだけにタグが付いている場合は、タグのないものが最低優先度となり、config で定義された順序で優先されます。
  * *どの*ボリュームにもタグが付いていない場合は、優先度は configuration で宣言された順序に応じて設定されます。
  * 2 つのボリュームが同じ優先度の値を持つことはできません。

設定例:

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

この例では、`hdd_in_order` ポリシーは [ラウンドロビン](https://en.wikipedia.org/wiki/Round-robin_scheduling) 方式を実装しています。したがって、このポリシーでは 1 つのボリューム (`single`) だけを定義し、データパーツはそのすべてのディスクに順番に循環しながら保存されます。このようなポリシーは、同種のディスクがシステムに複数マウントされているものの、RAID が構成されていない場合に非常に有用です。ただし、個々のディスクドライブ自体の信頼性は高くないため、レプリケーション係数を 3 以上にして補うことを検討してください。

システムで異なる種類のディスクが利用できる場合は、代わりに `moving_from_ssd_to_hdd` ポリシーを使用できます。`hot` ボリュームは SSD ディスク (`fast_ssd`) で構成され、このボリュームに保存できるパーツの最大サイズは 1GB です。1GB を超えるすべてのパーツは、HDD ディスク `disk1` を含む `cold` ボリュームに直接保存されます。
また、ディスク `fast_ssd` の使用率が 80% を超えると、バックグラウンドプロセスによってデータが `disk1` に移動されます。

ストレージポリシー内でのボリュームの列挙順は、列挙されているボリュームの少なくとも 1 つに明示的な `volume_priority` パラメーターがない場合に重要です。
あるボリュームがいっぱいになると、データは次のボリュームへ移動されます。ディスクの列挙順も重要で、データはそれらに順番に保存されるためです。

テーブルの作成時には、構成済みのストレージポリシーのいずれか 1 つを適用できます：

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

`default` ストレージポリシーは、1 つのボリュームだけを使用することを意味します。そのボリュームは、`<path>` で指定された 1 つのディスクだけで構成されます。
テーブル作成後でも、[ALTER TABLE ... MODIFY SETTING] クエリを使って ストレージポリシー を変更できます。新しい policy には、以前のディスクとボリュームを同じ名前ですべて含める必要があります。

データパーツのバックグラウンド移動を実行するスレッド数は、[background&#95;move&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings.md/#background_move_pool_size) 設定で変更できます。

<div id="details">
  ### 詳細
</div>

`MergeTree` テーブルでは、データはいくつかの経路でディスクに書き込まれます。

* insert (`INSERT` クエリ) の結果として。
* バックグラウンドマージおよび [mutations](/ja/sql-reference/statements/alter#mutations) の実行中。
* 別のレプリカからのダウンロード時。
* パーティションのフリーズ [ALTER TABLE ... FREEZE PARTITION](/ja/sql-reference/statements/alter/partition#freeze-partition) の結果として。

これらのうち、mutations とパーティションのフリーズを除くすべてのケースでは、指定されたストレージポリシーに従って、パーツはボリュームとディスクに保存されます。

1. パーツの保存に十分なディスク空き容量があり (`unreserved_space > current_part_size`) 、かつ指定されたサイズのパーツ保存が許可されている (`max_data_part_size_bytes > current_part_size`) 最初のボリューム (定義順) が選択されます。
2. このボリューム内では、前のデータ chunk の保存に使用されたディスクの次にあたるディスクのうち、パーツサイズを上回る空き領域があるもの (`unreserved_space - keep_free_space_bytes > current_part_size`) が選択されます。

内部的には、mutations とパーティションのフリーズでは [hard links](https://en.wikipedia.org/wiki/Hard_link) が使われます。異なるディスク間のハードリンクはサポートされていないため、このような場合、生成されるパーツは元のパーツと同じディスクに保存されます。

バックグラウンドでは、設定ファイルで宣言されたボリュームの順序に従い、空き領域の量 (`move_factor` パラメータ) に基づいて、パーツがボリューム間で移動されます。
データが最後のボリュームから転送されることも、最初のボリュームへ転送されることもありません。バックグラウンドでの移動は、システムテーブル [system.part&#95;log](/ja/operations/system-tables/part_log) (フィールド `type = MOVE_PART`) および [system.parts](/ja/operations/system-tables/parts.md) (フィールド `path` と `disk`) を使って監視できます。また、詳細な情報はサーバーログでも確認できます。

ユーザーは、クエリ [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/ja/sql-reference/statements/alter/partition) を使って、パーツまたはパーティションをあるボリュームから別のボリュームへ強制的に移動できます。このとき、バックグラウンド操作に対するすべての制約が考慮されます。このクエリは独自に移動を開始し、バックグラウンド操作の完了は待ちません。十分な空き領域がない場合、または必要な条件のいずれかが満たされていない場合、ユーザーにはエラーメッセージが返されます。

データの移動はデータのレプリケーションに影響しません。したがって、同じテーブルに対して、異なるレプリカで異なるストレージポリシーを指定できます。

バックグラウンドマージと mutations の完了後、古いパーツは一定時間 (`old_parts_lifetime`) が経過してからでないと削除されません。
この間、それらが他のボリュームやディスクに移動されることはありません。したがって、パーツが最終的に削除されるまでは、使用済みディスク容量の評価に引き続き含まれます。

ユーザーは、[min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/ja/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod) 設定を使用して、[JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) ボリューム内の異なるディスクに新しい大きなパーツをバランスよく割り当てることができます。

<div id="table_engine-mergetree-s3">
  ## データの保存に外部ストレージを使用する
</div>

[MergeTree](/ja/engines/table-engines/mergetree-family/mergetree.md) family のテーブルエンジンでは、`s3`、`azure_blob_storage`、`hdfs` 型のディスクを使用して、それぞれ `S3`、`AzureBlobStorage`、`HDFS` にデータを保存できます。詳細は、[外部ストレージオプションの設定](/ja/operations/storing-data.md/#configuring-external-storage)を参照してください。

`S3` を外部ストレージとして使用し、`s3` 型のディスクを利用する例を示します。詳しくは [S3](https://aws.amazon.com/s3/) をご覧ください。

設定マークアップ:

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

あわせて参照 [外部ストレージオプションの設定](/ja/operations/storing-data.md/#configuring-external-storage).

<div id="s3-multiple-volumes">
  ### 複数のボリュームで S3 ディスクを使用する
</div>

S3 (およびその他のオブジェクトストレージ) ディスクは、ローカルディスクと同様に、複数ディスクおよび複数ボリュームのストレージポリシーで使用できます。これにより、1 つのボリューム内で複数の S3 バケットにデータを分散させる (JBOD 形式) ことや、S3 ボリュームを使用した階層型ストレージポリシーを設定したりできます。

たとえば、2 つの S3 バケットにデータをラウンドロビン方式で分散するには、次のようにします。

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

階層型ポリシーでは、ローカルボリュームとS3ボリュームを組み合わせることもできます。たとえば、データの経過に応じてローカルSSDからS3へ移動させることができます:

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
S3 認証で `use_environment_credentials` を使用する場合、環境認証情報 (`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`) はすべての S3 ディスクで共有されます。ディスクごとに異なる環境認証情報を使うことはできません。S3 ディスクごとに別の認証情報が必要な場合は、代わりに各ディスクで `access_key_id` と `secret_access_key` を明示的に設定してください。
:::

共有ストレージでは、1 ライター・多数リーダー構成で、レプリケーションなしの MergeTree テーブルを設定できます。これは、リーダー側で設定できるパーツ一覧の自動更新によって実現されます。なお、これにはレプリカ間でファイルシステムのメタデータを共有する必要があります (または、テーブルローカルなディスクで `table_disk = true` を使用します) 。詳細は [refresh&#95;parts&#95;interval and table&#95;disk](/ja/operations/storing-data.md/#refresh-parts-interval-and-table-disk) を参照してください。

:::note cache configuration
ClickHouse 22.3 から 22.7 では、cache の構成が異なります。これらのバージョンを使用している場合は、[using local cache](/ja/operations/storing-data.md/#using-local-cache) を参照してください。
:::

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_part` — パート名。
* `_part_index` — クエリ結果内でのパートの連番索引。
* `_part_starting_offset` — クエリ結果内でのパートの累積開始行。
* `_part_offset` — パート内の行番号。
* `_part_granule_offset` — パート内のグラニュール番号。
* `_partition_id` — パーティション名。
* `_part_uuid` — 一意のパート識別子 (MergeTree 設定 `assign_part_uuids` が有効な場合) 。
* `_part_data_version` — パートのデータバージョン (最小 block 番号または mutation バージョン) 。
* `_partition_value` — `partition by` 式の値 (タプル) 。
* `_sample_factor` — サンプル係数 (クエリ由来) 。
* `_block_number` — 挿入時に行に割り当てられた元の block 番号。設定 `enable_block_number_column` が有効な場合は、マージ時にも保持されます。
* `_block_offset` — 挿入時に block 内の行に割り当てられた元の行番号。設定 `enable_block_offset_column` が有効な場合は、マージ時にも保持されます。
* `_disk_name` — ストレージに使用されるディスク名。

<div id="column-statistics">
  ## 列統計
</div>

<CloudNotSupportedBadge />

統計の宣言は、`*MergeTree*` ファミリーのテーブルに対する `CREATE` クエリのカラム定義セクションにあります。

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

`ALTER`文を使用して統計情報を操作することもできます。

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

これらの軽量な統計情報は、カラム内の値の分布に関する情報を集約したものです。統計情報は各パートに保存され、insert のたびに更新されます。
これらは、`set use_statistics = 1` を有効にした場合にのみ、PREWHERE 最適化に使用できます。

<div id="part-pruning-with-statistics">
  #### 統計情報を使用したパーツ剪枝
</div>

`use_statistics_for_part_pruning` が有効な場合、統計情報をパーツ剪枝に利用できます。
現在、パーツ剪枝をサポートしている統計情報は `MinMax` と `Basic` のみです。こうした統計情報がカラムに定義されている場合、ClickHouse は各パーツについて、そのカラムの最小値と最大値を追跡します。
パーツ剪枝を使用すると、クエリのフィルタ条件にそのパーツ内のどの行も一致しないことが明らかな場合、データパーツ全体の読み取りをスキップできます。

**例:**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### 利用可能なカラム STATISTICS の種類
</div>

* `Basic`

  カラムから導かれる単一値の要約をまとめた、compact なバンドルです。カラム type に応じて、次の情報が格納されます。

  * 値が数値で表される任意のカラム (整数、浮動小数点数、`Decimal*`、`Date*`、`DateTime*`、`Enum*`、`IPv4`、...) : 最小値と最大値。これにより、range filter の selectivity を見積もり、パーツ剪枝を有効にできます。
  * `String` および `FixedString` カラム: 非 `NULL` 値の合計バイト長 (ここから平均文字列長を求められます) 。
  * `Nullable` および `LowCardinality(Nullable)` カラム: `NULL` 値の件数。オプティマイザはこれを使って、selectivity の推定から `NULL` 行を差し引きます。

    1 つの `Basic` STATISTICS で、これらの複数を同時に格納できます。たとえば `Nullable(UInt32)` カラムでは、数値の最小値/最大値と `NULL` 件数の両方を追跡します。`MinMax` と比べると、`Basic` は `String` / `FixedString` カラムでも利用でき、さらに `UUID` や `IPv6` のような type の `Nullable` wrapper に対して、`NULL` 件数だけを追跡する目的で宣言することもできます。

    構文: `basic`

* `MinMax`

  数値カラムに対する range filter の selectivity を見積もるために使用できる、カラムの最小値と最大値です。

  構文: `minmax`

* `TDigest`

:::warning
`tdigest` type の STATISTICS は作成コストが高く、データの取り込みが遅くなる可能性があります。
:::

数値カラムに対して近似パーセンタイル (例: 90 パーセンタイル) を計算できる [TDigest](https://github.com/tdunning/t-digest) スケッチです。

構文: `tdigest`

* `Uniq`

  カラムに含まれる異なる値の数を概算できる [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) スケッチです。

  構文: `uniq`

* `CountMin`

:::warning
`countmin` type の STATISTICS は作成コストが高く、データの取り込みが遅くなる可能性があります。
:::

カラム内の各値の出現頻度を近似的に数える [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) スケッチです。

構文: `countmin`

<div id="supported-data-types">
  ### サポートされているデータ型
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | IPv4 | String or FixedString |
| -------- | -------------------------------------------------- | ---- | --------------------- |
| Basic    | ✔                                                  | ✔    | ✔                     |
| CountMin | ✔                                                  | ✔    | ✔                     |
| MinMax   | ✔                                                  | ✔    | ✗                     |
| TDigest  | ✔                                                  | ✗    | ✗                     |
| Uniq     | ✔                                                  | ✔    | ✔                     |

上記はすべて、記載された型の `Nullable` および `LowCardinality(Nullable)` ラッパーにも対応しています。`Basic` は、NULL の件数を追跡する目的に限り、`UUID` や `IPv6` などの型の `Nullable` ラッパーに対して追加で宣言することもできます。

<div id="supported-operations">
  ### サポートされる操作
</div>

|          | 等価フィルタ (==) | 範囲フィルタ (`>, >=, <, <=`) |
| -------- | ----------- | ----------------------- |
| Basic    | ✗           | ✔ (数値カラムのみ)             |
| CountMin | ✔           | ✗                       |
| MinMax   | ✗           | ✔ (数値カラムのみ)             |
| TDigest  | ✗           | ✔ (数値カラムのみ)             |
| Uniq     | ✔           | ✗                       |

`String` / `FixedString` カラムで `Basic` を使用する場合、この統計で記録されるのは、
非 NULL の合計バイト長 (平均文字列長の推定に使用) と NULL の数のみです。
範囲フィルタやパーツ剪枝には使用されません。

<div id="column-level-settings">
  ## カラムレベル設定
</div>

一部の MergeTree 設定は、カラムレベルで上書きできます。

* `max_compress_block_size` — テーブルに書き込む際、圧縮前の非圧縮データブロックの最大サイズ。
* `min_compress_block_size` — 次のマークを書き込む際に圧縮を行うために必要な、非圧縮データブロックの最小サイズ。

例:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

カラムレベル設定は、たとえば [ALTER MODIFY COLUMN](/ja/sql-reference/statements/alter/column.md) を使って変更または削除できます。

* カラム宣言から `SETTINGS` を削除する場合:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* 設定を変更します：

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* 1 つ以上の設定をリセットし、テーブルの CREATE クエリ内にあるカラム式での設定宣言も削除します。

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```