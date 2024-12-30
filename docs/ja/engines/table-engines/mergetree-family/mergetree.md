---
slug: /ja/engines/table-engines/mergetree-family/mergetree
sidebar_position: 11
sidebar_label: MergeTree
---

# MergeTree

`MergeTree`エンジンおよび`MergeTree`ファミリーの他のエンジン（例えば、`ReplacingMergeTree`、`AggregatingMergeTree`）は、ClickHouseで最も一般的に使用され、最も堅牢なテーブルエンジンです。

`MergeTree`ファミリーのテーブルエンジンは、高いデータ取り込み速度と膨大なデータ量を処理するために設計されています。
データの挿入操作はテーブルのパーツを作成し、これらのパーツはバックグラウンドプロセスで他のテーブルパーツとマージされます。

`MergeTree`ファミリーのテーブルエンジンの主な特徴:

- テーブルの主キーは、各テーブルパーツ内のソート順を決定します（クラスター化インデックス）。主キーは8192行のブロック（グラニュールと呼ばれる）を参照し、個々の行を参照しません。これにより、大規模なデータセットの主キーがメインメモリに保持できるほど小さくなる一方で、ディスクに保存されたデータへの迅速なアクセスを提供します。

- テーブルは任意のパーティション式を使用してパーティションを分けることができます。パーティションプルーニングにより、クエリが許す場合に読み取りからパーティションを除外できます。

- データは高可用性、フェイルオーバー、ダウンタイムゼロでのアップグレードのために複数のクラスターのノードにまたがってレプリケートできます。[データレプリケーション](/docs/ja/engines/table-engines/mergetree-family/replication.md)を参照してください。

- `MergeTree`テーブルエンジンは、クエリ最適化を支援するために様々な統計情報やサンプリング方法をサポートします。

:::note
似た名前ですが、[Merge](/docs/ja/engines/table-engines/special/merge.md/#merge)エンジンは`*MergeTree`エンジンとは異なります。
:::

## テーブルの作成 {#table_engine-mergetree-creating-a-table}

``` sql
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

パラメータの詳細な説明は、[CREATE TABLE](/docs/ja/sql-reference/statements/create/table.md)ステートメントを参照してください。

### クエリ句 {#mergetree-query-clauses}

#### ENGINE

`ENGINE` — エンジンの名前とパラメータ。`ENGINE = MergeTree()`。`MergeTree`エンジンにはパラメータはありません。

#### ORDER_BY

`ORDER BY` — ソートキー。

カラム名や任意の式のタプル。例: `ORDER BY (CounterID + 1, EventDate)`。

主キーが定義されていない場合（つまり、`PRIMARY KEY`が指定されていない場合）、ClickHouseはソートキーを主キーとして使用します。

ソートの必要がない場合は、`ORDER BY tuple()`という構文を使用できます。
また、`create_table_empty_primary_key_by_default`設定を有効にすると、`CREATE TABLE`ステートメントに`ORDER BY tuple()`が暗黙的に追加されます。[主キーの選択](#selecting-a-primary-key)を参照してください。

#### PARTITION BY

`PARTITION BY` — [パーティションキー](/docs/ja/engines/table-engines/mergetree-family/custom-partitioning-key.md)。オプションです。多くの場合、パーティションキーは必要ありませんが、パーティション分けが必要な場合でも一般的には月ごとのパーティションキー以上の粒度が必要ありません。パーティション分けはクエリを速くするものではありません（`ORDER BY`式とは対照的に）。過度に細かいパーティション分けは避ける必要があります。クライアントの識別子や名前でデータをパーティション分けしないでください（代わりにクライアントの識別子または名前を`ORDER BY`式の最初のカラムに設定してください）。

月によるパーティション分けには、`toYYYYMM(date_column)`式を使用します。ここで`date_column`は[Date](/docs/ja/sql-reference/data-types/date.md)型の日付を持つカラムです。パーティション名は`"YYYYMM"`形式になります。

#### PRIMARY KEY

`PRIMARY KEY` — [ソートキーと異なる主キーを選択する](#choosing-a-primary-key-that-differs-from-the-sorting-key)場合の主キー。オプションです。

ソートキーを指定する（`ORDER BY`句を使用する）ことで、暗黙的に主キーが指定されます。
ソートキーに加えて主キーを指定する必要は通常ありません。

#### SAMPLE BY

`SAMPLE BY` — サンプリング式。オプションです。

指定されている場合は、主キーに含まれている必要があります。
サンプリング式は符号なし整数を結果とする必要があります。

例: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`。

#### TTL

`TTL` — 行の保存期間と自動パーツ移動のロジックを指定するルールのリスト [ディスクとボリューム間](#table_engine-mergetree-multiple-volumes)。オプションです。

式は`Date`または`DateTime`である必要があります。例として `TTL date + INTERVAL 1 DAY`。

ルールの種類`DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY`は、式が満たされた場合（現在時刻に達する）、そのパートに対して行われるアクションを指定します：期限切れの行の削除、特定ディスクへのパートの移動（`TO DISK 'xxx'`）、またはボリューム（`TO VOLUME 'xxx'`）への移動、または期限切れ行の集計。デフォルトのルールタイプは削除（`DELETE`）です。複数のルールを設定できますが、`DELETE`ルールは1つだけにする必要があります。

詳細については、[カラムとテーブルのTTL](#table_engine-mergetree-ttl)を参照してください。

#### SETTINGS

[MergeTree設定](../../../operations/settings/merge-tree-settings.md)を参照してください。

**セクション設定の例**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

例では、月によるパーティション分けを設定しています。

また、ユーザーIDでのハッシュとしてサンプリングの式も設定しています。これにより、各`CounterID`および`EventDate`のためのテーブル内のデータを疑似ランダムに化すことができます。データの選択時に[SAMPLE](/docs/ja/sql-reference/statements/select/sample.md/#select-sample-clause)句を定義する場合、ClickHouseはユーザーのサブセットに対して均一な疑似ランダムデータサンプルを返します。

`index_granularity`設定は省略可能です。デフォルト値は8192です。

<details markdown="1">

<summary>テーブル作成の非推奨メソッド</summary>

:::note
新しいプロジェクトでこの方法を使用しないでください。可能であれば、古いプロジェクトを上記の方法に切り替えてください。
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree()のパラメータ**

- `date-column` — [Date](/docs/ja/sql-reference/data-types/date.md)型のカラム名。ClickHouseはこのカラムに基づいて月ごとにパーティションを自動作成します。パーティション名は`"YYYYMM"`形式です。
- `sampling_expression` — サンプリングのための式。
- `(primary, key)` — 主キー。型：[Tuple()](/docs/ja/sql-reference/data-types/tuple.md)
- `index_granularity` — インデックスの粒度。インデックスの「マーク」の間のデータ行数。8192という値は、ほとんどのタスクに適しています。

**例**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

`MergeTree`エンジンは、上記の主要エンジン設定方法の例と同じ方法で構成されます。
</details>

## データストレージ {#mergetree-data-storage}

テーブルは、主キーでソートされたデータパートから構成されます。

テーブルにデータが挿入されると、個別のデータパートが作成され、それぞれが主キーで辞書順にソートされます。例えば、主キーが`(CounterID, Date)`の場合、パート内のデータは`CounterID`でソートされ、各`CounterID`内で`Date`で並べ替えられます。

異なるパーティションに属するデータは、異なるパートに分離されます。バックグラウンドで、ClickHouseはデータパートをマージして、より効率的に保管します。異なるパーティションに属するパーツはマージされません。マージメカニズムは、同じ主キーを持つ全ての行が同じデータパートに含まれることを保証しません。

データパートは`Wide`または`Compact`フォーマットで保存できます。`Wide`フォーマットでは、各カラムはファイルシステム内の別々のファイルに保存され、`Compact`フォーマットでは全てのカラムが1つのファイルに保存されます。`Compact`フォーマットは、小規模で頻繁な挿入のパフォーマンスを向上させるために使用できます。

データの保存フォーマットは、テーブルエンジンの`min_bytes_for_wide_part`と`min_rows_for_wide_part`設定によって制御されます。データパートのバイトまたは行数が対応する設定値より少ない場合、パートは`Compact`フォーマットで保存されます。それ以外の場合は`Wide`フォーマットで保存されます。これらの設定が設定されていない場合、データパートは`Wide`フォーマットで保存されます。

各データパートは、論理的にグラニュールに分割されます。グラニュールは、ClickHouseがデータを選択する際に読み取る最小の不可分データセットです。ClickHouseは行や値を分割しないため、各グラニュールには常に整数の行数が含まれます。グラニュールの最初の行は、その行の主キーの値でマークされています。各データパートについて、ClickHouseはインデックスファイルを作成し、マークを保存します。プライマリキーにあるかどうかにかかわらず、各カラムについても同様のマークが保存されます。これらのマークにより、カラムファイル内のデータを直接見つけることができます。

グラニュールのサイズは、テーブルエンジンの`index_granularity`と`index_granularity_bytes`設定によって制限されます。グラニュール内の行の数は`[1, index_granularity]`範囲内にあり、行のサイズによって異なります。グラニュールのサイズは、単一の行のサイズが設定値より大きい場合、`index_granularity_bytes`を超えることがあります。この場合、グラニュールのサイズは行のサイズに等しくなります。

## クエリでの主キーとインデックス {#primary-keys-and-indexes-in-queries}

例えば、`(CounterID, Date)`の主キーを考えます。この場合、ソートとインデックスは次のように図示されます：

      全データ:     [---------------------------------------------]
      CounterID:    [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:         [1111111222222233331233211111222222333211111112122222223111112223311122333]
      マーク:         |      |      |      |      |      |      |      |      |      |      |
                    a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      マーク番号:   0      1      2      3      4      5      6      7      8      9      10

データクエリが次を指定している場合：

- `CounterID in ('a', 'h')`、サーバーはマークの範囲`[0, 3)`と`[6, 8)`のデータを読み取ります。
- `CounterID IN ('a', 'h') AND Date = 3`、サーバーはマークの範囲`[1, 3)`と`[7, 8)`のデータを読み取ります。
- `Date = 3`、サーバーはマークの範囲`[1, 10]`のデータを読み取ります。

上記の例は、フルスキャンよりもインデックスを使用する方が常に効果的であることを示しています。

訳注：

スパースインデックスは、追加のデータを読み取ることを許可します。プライマリキーの単一範囲を読み取る際に、データブロックごとに最大`index_granularity * 2`の追加行を読み取ることができます。

スパースインデックスにより、大量のテーブル行を使用することができます。なぜなら、ほとんどの場合、このようなインデックスはコンピューターのRAMに収まるからです。

ClickHouseは一意のプライマリキーを必要としません。同じプライマリキーを持つ複数の行を挿入できます。

`Nullable`型の式を`PRIMARY KEY`と`ORDER BY`句に使用できますが、強く推奨されません。この機能を許可するには、[allow_nullable_key](/docs/ja/operations/settings/settings.md/#allow-nullable-key)設定をオンにします。`ORDER BY`句における`NULL`値には[NULLS_LAST](/docs/ja/sql-reference/statements/select/order-by.md/#sorting-of-special-values)原則が適用されます。

### 主キーの選択 {#selecting-a-primary-key}

主キーに含まれるカラムの数に明示的な制限はありません。データ構造によって、プライマリキーに多くのカラムを含めることも、少ないカラムを含めることもできます。これにより以下のことが可能です：

- インデックスのパフォーマンスを向上させる。

    主キーが`(a, b)`の場合、以下の条件が満たされると、追加のカラム`c`を追加することでパフォーマンスが向上します：

    - カラム`c`に対する条件を持つクエリが存在する。
    - `(a, b)`の値が同一の長いデータ範囲（`index_granularity`の数倍程度の長さ）が一般的である。つまり、長いデータ範囲をスキップできる場合に、追加のカラムを追加する意味があります。

- データ圧縮の向上。

    ClickHouseはプライマリキーによってデータをソートするため、一貫性が高いほど、圧縮が向上します。

- [CollapsingMergeTree](/docs/ja/engines/table-engines/mergetree-family/collapsingmergetree.md/#table_engine-collapsingmergetree)および[SummingMergeTree](/docs/ja/engines/table-engines/mergetree-family/summingmergetree.md)エンジンでデータパーツをマージする際の追加ロジックを提供します。

    この場合、主キーとは異なる*ソートキー*を指定することには意義があります。

長いプライマリキーは挿入パフォーマンスとメモリ消費に悪影響を与えますが、追加のカラムは`SELECT`クエリのパフォーマンスには影響しません。

`ORDER BY tuple()`構文を使用して、プライマリキーなしのテーブルを作成することができます。この場合、ClickHouseは挿入の順序でデータを保存します。`INSERT ... SELECT`クエリでデータを挿入するときにデータの順序を保持したい場合は、[max_insert_threads = 1](/docs/ja/operations/settings/settings.md/#max-insert-threads)を設定します。

初期の順序でデータを選択するには、[単一スレッド](/docs/ja/operations/settings/settings.md/#max_threads)の`SELECT`クエリを使用します。

### ソートキーと異なる主キーの選択 {#choosing-a-primary-key-that-differs-from-the-sorting-key}

プライマリキー（マークごとにインデックスファイルに書き込まれる値の式）が、ソートキー（データパーツ内の行をソートするための式）と異なる場合に指定することができます。この場合、プライマリキーの式タプルはソートキーの式タプルのプレフィックスである必要があります。

この機能は[SummingMergeTree](/docs/ja/engines/table-engines/mergetree-family/summingmergetree.md)や[AggregatingMergeTree](/docs/ja/engines/table-engines/mergetree-family/aggregatingmergetree.md)のエンジンで使用する際に役立ちます。通常、これらのエンジンを使用する場合、テーブルには*ディメンション*と*メジャー*の2種類のカラムがあります。典型的なクエリでは、ディメンションを`GROUP BY`およびフィルタリングして、メジャーカラムの値を集計します。SummingMergeTreeとAggregatingMergeTreeは、ソートキーの値が同じ行を集計するため、ディメンションの全てをソートキーに追加するのは自然です。この結果、キーの式は長いカラムリストとなり、新たに追加されたディメンションで頻繁に更新する必要があります。

この場合、効率的な範囲スキャンを提供する少数のカラムだけをプライマリキーに残し、残りのディメンションカラムをソートキータプルに追加するのが賢明です。

ソートキーの[ALTER](/docs/ja/sql-reference/statements/alter/index.md)は軽量な操作です。新しいカラムがテーブルに追加され、同時にソートキーに追加されるとき、既存のデータパーツを変更する必要はありません。古いソートキーが新しいソートキーのプレフィックスであり、新しく追加されたカラムに既存のデータがないため、テーブル変更の瞬間にデータは両方の古いと新しいソートキーでソートされます。

### クエリにおけるインデックスとパーティションの利用 {#use-of-indexes-and-partitions-in-queries}

`SELECT`クエリに対して、ClickHouseはインデックスが使用可能かどうかを分析します。インデックスは、`WHERE/PREWHERE`句にプライマリキーまたはパーティションキーに含まれるカラムや式、またはこれらの特定の部分的に繰り返される関数が含まれる場合に使用可能です。また、これらの式やそれらの論理的な関係が含まれる場合に使用されます。

したがって、プライマリキーの1つまたは複数の範囲でクエリを迅速に実行することが可能です。この例では、特定のトラッキングタグ、特定のタグと日付範囲、特定のタグと日付、複数のタグと日付範囲などに対するクエリが高速になります。

次のようにエンジンが設定されている場合を見てみましょう：
```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

この場合、クエリでは：

``` sql
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

ClickHouseはプライマリキーインデックスを使用して不適切なデータをトリムし、月ごとのパーティションキーを使用して不適切な日付範囲のパーティションをトリムします。

上記のクエリは、複雑な式でもインデックスが使用されることを示しています。テーブルからの読み取りは、インデックスの使用がフルスキャンより遅くなることがないように整理されています。

以下の例では、インデックスを使用できません。

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

クエリを実行する際にClickHouseがインデックスを使用できるかどうかを確認するために、[force_index_by_date](/docs/ja/operations/settings/settings.md/#force_index_by_date)および[force_primary_key](/docs/ja/operations/settings/settings.md/#force-primary-key)設定を使用してください。

月ごとのパーティションでパーティション分けを行うキーは、適切な範囲のデータブロックのみを読み取ることを可能にします。この場合、データブロックには多くの日付（場合によっては丸ごと月まで）が含まれることがあります。ブロック内では、データはプライマリキーで並べ替えられますが、日付が最初のカラムとして含まれていない可能性があります。したがって、プライマリキーのプレフィックスを指定しないただの日付条件でのクエリの使用は、単一の日付よりも多くのデータを読み取る原因となります。

### 部分的に単調なプライマリキーのインデックス使用 {#use-of-index-for-partially-monotonic-primary-keys}

例えば、月の日数を考えてみましょう。一ヶ月に対しては[単調なシーケンス](https://en.wikipedia.org/wiki/Monotonic_function)を形成しますが、より長い期間に対しては単調ではありません。これは部分的に単調なシーケンスです。ユーザーが部分的に単調なプライマリキーでテーブルを作成した場合、ClickHouseは通常のようにスパースインデックスを作成します。ユーザーがこの種のテーブルからデータを選択する際、ClickHouseはクエリ条件を分析します。ユーザーがインデックスの2つのマーク間のデータを取得し、両マークが1ヶ月内に収まる場合、ClickHouseはこの特定のケースでインデックスを使用できます。なぜなら、クエリパラメータとインデックスマーク間の距離を計算できるからです。

ClickHouseは、クエリパラメータ範囲内のプライマリキーの値が単調シーケンスを表していない場合、インデックスを使用できません。この場合、ClickHouseはフルスキャンメソッドを使用します。

ClickHouseは、このロジックを月の日シーケンスだけでなく、部分的に単調なシーケンスを表すあらゆるプライマリキーに対しても使用します。

### データスキッピングインデックス {#table_engine-mergetree-data_skipping-indexes}

インデックス宣言は`CREATE`クエリのカラムセクションで行われます。

``` sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

`*MergeTree`ファミリーのテーブルでは、データスキッピングインデックスを指定できます。

これらのインデックスは、グランラリティ値`granularity_value`のグラニュールで構成されたブロック上で指定された式についての情報を集計し、集計結果を`SELECT`クエリで使用して、クエリが満たすことができない大きなデータブロックをスキップすることでディスクから読み取るデータ量を削減します。

`GRANULARITY`句は省略可能で、`granularity_value`のデフォルト値は1です。

**例**

``` sql
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

例に示されているインデックスは、以下のクエリにおいてディスクから読み取るデータ量を削減するためにClickHouseによって利用されることがあります：

``` sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

データスキッピングインデックスは、複合集合カラムに対しても作成することができます：

```sql
-- Map型のカラムに対して:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- Tuple型のカラムに対して:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- Nested型のカラムに対して:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

### 使用可能なインデックスタイプ {#available-types-of-indices}

#### MinMax

指定された式の極値を保存します（もし式が`tuple`の場合は、`tuple`の各要素の極値を保存します）。保存された情報を利用して、プライマリキーのようにデータブロックをスキップします。

構文: `minmax`

#### Set

指定された式のユニークな値を保存します（最大行数が`max_rows`行で、`max_rows=0`は「制限なし」を意味します）。その値を使用して、`WHERE`式がデータブロックで満たせない場合を確認します。

構文: `set(max_rows)`

#### ブルームフィルタ

指定されたカラムに対して[ブルームフィルタ](https://en.wikipedia.org/wiki/Bloom_filter)を保存します。オプションの`false_positive`パラメータは、0から1の値でフィルタから偽陽性の応答を受け取る確率を指定します。デフォルト値: 0.025。サポートされているデータ型： `Int*`、 `UInt*`、 `Float*`、 `Enum`、 `Date`、 `DateTime`、 `String`、 `FixedString`、 `Array`、 `LowCardinality`、 `Nullable`、 `UUID`、 `Map`。`Map`データ型に対しては、クライアントは[mapKeys](/docs/ja/sql-reference/functions/tuple-map-functions.md/#mapkeys)または[mapValues](/docs/ja/sql-reference/functions/tuple-map-functions.md/#mapvalues)関数を使用して、キーまたは値に対してインデックスを作成すべきかを指定できます。

構文: `bloom_filter([false_positive])`

#### N-gramブルームフィルタ

ブロックデータから全てのn-gramを含む[ブルームフィルタ](https://en.wikipedia.org/wiki/Bloom_filter)を保存します。データ型が[String](/docs/ja/sql-reference/data-types/string.md)、[FixedString](/docs/ja/sql-reference/data-types/fixedstring.md)、[Map](/docs/ja/sql-reference/data-types/map.md)に対応します。`EQUALS`、`LIKE`、`IN`式の最適化に使用できます。

構文: `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

- `n` — ngramのサイズ、
- `size_of_bloom_filter_in_bytes` — ブルームフィルタのサイズ（大きな値を使用できます。例えば、256または512は圧縮効率が良いです）。
- `number_of_hash_functions` — ブルームフィルタで使用されるハッシュ関数の数。
- `random_seed` — ブルームフィルタのハッシュ関数のシード。

ユーザーは`ngrambf_v1`のパラメータセットを推定するために[UDF](/docs/ja/sql-reference/statements/create/function.md)を作成できます。以下に示すクエリ文を実行します：

```sql
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
それらの関数を使用するためには、少なくとも2つのパラメータを指定する必要があります。
例えば、グラニュールで4300ngramがあり、偽陽性を0.0001以下に予期している場合です。他のパラメーターは以下のクエリを実行することで推定できます：

```sql
--- フィルタ内のビット数を推定
SELECT bfEstimateBmSize(4300, 0.0001) / 8 as size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- ハッシュ関数の数を推定
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘

```
もちろん、これらの関数を使用して、他の条件に基づいたパラメータの推定も可能です。
これら関数の内容は[こちら](https://hur.st/bloomfilter)を参照してください。

#### トークンブルームフィルタ

`ngrambf_v1`と同様だが、トークンを格納します。トークンは非英数字文字で区切られた文字列です。

構文: `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

#### 特殊目的

- 近似最近隣（ANN）検索をサポートするためのエクスペリメンタルなインデックス。詳細は[こちら](annindexes.md)を参照してください。
- フルテキスト検索をサポートするためのエクスペリメンタルなフルテキストインデックス。詳細は[こちら](invertedindexes.md)を参照してください。

### サポートされている機能 {#functions-support}

`WHERE`句の条件は、カラムを操作する関数の呼び出しを含みます。カラムがインデックスの一部である場合、ClickHouseは関数を実行する際にこのインデックスを利用しようとします。ClickHouseはインデックスに対して異なる関数のサブセットをサポートしています。

`set`型のインデックスは、すべての関数によって利用可能です。他のインデックスタイプは次のようにサポートされています：

| 関数（オペレーター）/ インデックス                                                                                    | プライマリキー | minmax | ngrambf_v1 | tokenbf_v1 | bloom_filter | full_text |
|--------------------------------------------------------------------------------------------------|-------------|--------|------------|------------|--------------|-----------|
| [equals (=, ==)](/docs/ja/sql-reference/functions/comparison-functions.md/#equals)                  | ✔           | ✔      | ✔          | ✔          | ✔            | ✔         |
| [notEquals(!=, &lt;&gt;)](/docs/ja/sql-reference/functions/comparison-functions.md/#notequals)     | ✔           | ✔      | ✔          | ✔          | ✔            | ✔         |
| [like](/docs/ja/sql-reference/functions/string-search-functions.md/#like)                            | ✔           | ✔      | ✔          | ✔          | ✗            | ✔         |
| [notLike](/docs/ja/sql-reference/functions/string-search-functions.md/#notlike)                      | ✔           | ✔      | ✔          | ✔          | ✗            | ✔         |
| [match](/docs/ja/sql-reference/functions/string-search-functions.md/#match)                          | ✗           | ✗      | ✔          | ✔          | ✗            | ✔         |
| [startsWith](/docs/ja/sql-reference/functions/string-functions.md/#startswith)                       | ✔           | ✔      | ✔          | ✔          | ✗            | ✔         |
| [endsWith](/docs/ja/sql-reference/functions/string-functions.md/#endswith)                           | ✗           | ✗      | ✔          | ✔          | ✗            | ✔         |
| [multiSearchAny](/docs/ja/sql-reference/functions/string-search-functions.md/#multisearchany)        | ✗           | ✗      | ✔          | ✗          | ✗            | ✔         |
| [in](/docs/ja/sql-reference/functions/in-functions)                                                      | ✔           | ✔      | ✔          | ✔          | ✔            | ✔         |
| [notIn](/docs/ja/sql-reference/functions/in-functions)                                                   | ✔           | ✔      | ✔          | ✔          | ✔            | ✔         |
| [less (<)](/docs/ja/sql-reference/functions/comparison-functions.md/#less)                               | ✔           | ✔      | ✗          | ✗          | ✗            | ✗         |
| [greater (>)](/docs/ja/sql-reference/functions/comparison-functions.md/#greater)                         | ✔           | ✔      | ✗          | ✗          | ✗            | ✗         |
| [lessOrEquals (<=)](/docs/ja/sql-reference/functions/comparison-functions.md/#lessorequals)              | ✔           | ✔      | ✗          | ✗          | ✗            | ✗         |
| [greaterOrEquals (>=)](/docs/ja/sql-reference/functions/comparison-functions.md/#greaterorequals)        | ✔           | ✔      | ✗          | ✗          | ✗            | ✗         |
| [empty](/docs/ja/sql-reference/functions/array-functions/#empty)                                        | ✔           | ✔      | ✗          | ✗          | ✗            | ✗         |
| [notEmpty](/docs/ja/sql-reference/functions/array-functions/#notempty)                                  | ✔           | ✔      | ✗          | ✗          | ✗            | ✗         |
| [has](/docs/ja/sql-reference/functions/array-functions/#has)                                            | ✗           | ✗      | ✔          | ✔          | ✔            | ✔         |
| [hasAny](/docs/ja/sql-reference/functions/array-functions/#hasany)                                      | ✗           | ✗      | ✔          | ✔          | ✔            | ✗         |
| [hasAll](/docs/ja/sql-reference/functions/array-functions/#hasall)                                      | ✗           | ✗      | ✗          | ✗          | ✔            | ✗         |
| hasToken                                                                                                     | ✗           | ✗      | ✗          | ✔          | ✗            | ✔         |
| hasTokenOrNull                                                                                               | ✗           | ✗      | ✗          | ✔          | ✗            | ✔         |
| hasTokenCaseInsensitive (*)                                                                                    | ✗           | ✗      | ✗          | ✔          | ✗            | ✗         |
| hasTokenCaseInsensitiveOrNull (*)                                                                            | ✗           | ✗      | ✗          | ✔          | ✗            | ✗         |

定数引数のある関数で、ngramサイズより小さいものは`ngrambf_v1`によってクエリ最適化には使用できません。

(*) `hasTokenCaseInsensitive`および`hasTokenCaseInsensitiveOrNull`が効果的であるためには、`tokenbf_v1`インデックスが小文字化されたデータに対して作成される必要があります。例えば`INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`。

:::note
ブルームフィルタは偽陽性マッチを持つ可能性があるため、`ngrambf_v1`、`tokenbf_v1`、`bloom_filter`インデックスは、関数の結果が偽であると予想されるクエリの最適化には使用できません。

例えば：

- 最適化可能：
    - `s LIKE '%test%'`
    - `NOT s NOT LIKE '%test%'`
    - `s = 1`
    - `NOT s != 1`
    - `startsWith(s, 'test')`
- 最適化不可能：
    - `NOT s LIKE '%test%'`
    - `s NOT LIKE '%test%'`
    - `NOT s = 1`
    - `s != 1`
    - `NOT startsWith(s, 'test')`
:::

## プロジェクション {#projections}

プロジェクションは、パートレベルで定義される[マテリアライズドビュー](/docs/ja/sql-reference/statements/create/view.md/#materialized)に似ています。一貫性のある保証とクエリでの自動使用を提供します。

:::note
プロジェクションを実装する際には、[force_optimize_projection](/docs/ja/operations/settings/settings.md/#force-optimize-projection)設定も考慮してください。
:::

プロジェクションは[FINAL](/docs/ja/sql-reference/statements/select/from.md/#select-from-final)修飾子を使用した`SELECT`ステートメントでサポートされません。

### プロジェクションクエリ {#projection-query}

プロジェクションクエリはプロジェクションを定義するものです。暗黙的に親テーブルからデータを選択します。

**構文**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

プロジェクションは、[ALTER](/docs/ja/sql-reference/statements/alter/projection.md)ステートメントで変更または削除できます。

### プロジェクションストレージ {#projection-storage}

プロジェクションはパートディレクトリ内に保存されます。これはインデックスに似ていますが、匿名の`MergeTree`テーブルのパートを格納するサブディレクトリを含みます。テーブルはプロジェクションの定義クエリによって誘導されます。`GROUP BY`句がある場合、基盤となるストレージエンジンは[AggregatingMergeTree](aggregatingmergetree.md)となり、全ての集計関数が`AggregateFunction`に変換されます。`ORDER BY`句がある場合、`MergeTree`テーブルはそれを主キー式として使用します。マージプロセス中にプロジェクションパートはそのストレージのマージルーチンを通じてマージされます。親テーブルのパートのチェックサムはプロジェクションのパートと結合されます。他のメンテナンスジョブもスキップインデックスと似ています。

### クエリアナリシス {#projection-query-analysis}

1. 特定のクエリに対してプロジェクションが使用可能かどうかを確認します。それがベーステーブルをクエリするのと同じ答えを生成すること。
2. 読み取るグラニュールが最も少ない最高の適合を選択します。
3. プロジェクションを使用するクエリパイプラインは、元のパーツを使用するものとは異なります。プロジェクションが一部のパーツに存在しない場合、それを即座に「プロジェクト」するためのパイプラインを追加できます。

## 同時データアクセス {#concurrent-data-access}

同時のテーブルアクセスには、マルチバージョニングを使用します。つまり、テーブルが同時に読み取りと更新される場合、クエリ時に現在のパーツセットからデータが読み取られます。長期間のロックはありません。挿入が読み取り操作を妨げません。

テーブルからの読み取りは自動的に並列化されます。

## カラムとテーブルのTTL {#table_engine-mergetree-ttl}

値の有効期間を決定します。

`TTL`句はテーブル全体および各カラムごとに設定できます。テーブルレベルの`TTL`はディスクとボリューム間の自動データ移動のロジックや、すべてのデータが期限切れになったパーツの再圧縮を指定することもできます。

式は[Date](/docs/ja/sql-reference/data-types/date.md)または[DateTime](/docs/ja/sql-reference/data-types/datetime.md)型である必要があります。

**構文**

カラムのTTLを設定：

``` sql
TTL time_column
TTL time_column + interval
```

`interval`を定義するには、[時間間隔](/docs/ja/sql-reference/operators/index.md#operators-datetime)演算子を使用します。例えば：

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### カラムTTL {#mergetree-column-ttl}

カラム内の値が期限切れになると、ClickHouseはそれらをカラムデータ型のデフォルト値に置き換えます。データパート内のすべてのカラム値が期限切れになると、ClickHouseはこのカラムをファイルシステム内のデータパートから削除します。

`TTL`句はキーのカラムには使用できません。

**例**

#### `TTL`を使用したテーブルの作成：

``` sql
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

#### 既存テーブルのカラムへのTTL追加

``` sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

#### カラムのTTLの変更

``` sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### テーブルTTL {#mergetree-table-ttl}

テーブルには期限切れの行を削除するための式、および[ディスクまたはボリューム間](#table_engine-mergetree-multiple-volumes)でパーツを自動移動するための複数の式を持つことができます。テーブル内の行が期限切れになると、ClickHouseは対応するすべての行を削除します。パーツの移動または再圧縮の場合、すべての行は`TTL`式の基準を満たさなければなりません。

``` sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

TTLルールのタイプは各TTL式に続くことができ、式が満たされた場合（現在の時刻に達する）、実行するアクションに影響を与えます：

- `DELETE` - 期限切れの行を削除する（デフォルトのアクション）。
- `RECOMPRESS codec_name` - `codec_name`でデータパートを再圧縮する。
- `TO DISK 'aaa'` - パートをディスク`aaa`に移動する。
- `TO VOLUME 'bbb'` - パートをディスク`bbb`に移動する。
- `GROUP BY` - 期限切れの行を集計する。

`DELETE`アクションは`WHERE`条件と共に使用することができ、既に期限が切れた特定の行のみをフィルタ条件に基づいて削除することができます：

``` sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY`式は、テーブルの主キーの接頭辞でなければなりません。

`GROUP BY`式に含まれないカラムで、`SET`句で明示的に設定されていないものは、結果行でグループ化された行から任意の値を含みます（agg関数`any`が適用されるかのよう）。

**例**

#### `TTL`を使用したテーブルの作成：

``` sql
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

#### テーブルの`TTL`の変更：

``` sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

行が1ヶ月後に失効するテーブルを作成し、期限切れの日付が月曜日である場合は行を削除する：

``` sql
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

#### 期限切れの行が再圧縮されるテーブルを作成：

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

期限切れの行が集計されるテーブルを作成。結果行で`x`はグループ化された行の最大値を含み、`y`は最小値を含み、`d`はグループ化された行からの任意の値を含みます。

``` sql
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

### 期限切れデータの削除 {#mergetree-removing-expired-data}

有効期限が切れたデータは、ClickHouseがデータパーツをマージするときに削除されます。

ClickHouseがデータが期限切れであることを検出すると、予定外のマージを実行します。このようなマージの頻度を制御するには、`merge_with_ttl_timeout`を設定できます。値が低すぎると、多くの予定外のマージが実行され、リソースを大量に消費する可能性があります。

マージの間に`SELECT`クエリを実行すると、期限切れのデータを取得する可能性があります。これを避けるためには、`SELECT`の前に[OPTIMIZE](/docs/ja/sql-reference/statements/optimize.md)クエリを使用してください。

**関連項目**

- [ttl_only_drop_parts](/docs/ja/operations/settings/settings.md/#ttl_only_drop_parts)設定

## ディスクタイプ

ClickHouseはローカルブロックデバイスに加え、以下のストレージタイプをサポートしています:
- [S3とMinIOの`s3`](#table_engine-mergetree-s3)
- [GCS用`gcs`](/docs/ja/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
- [Azure Blob Storage用`blob_storage_disk`](#table_engine-mergetree-azure-blob-storage)
- [HDFS用`hdfs`](#hdfs-storage)
- [Webからの読み取り専用用`web`](#web-storage)
- [ローカルキャッシュ用`cache`](/docs/ja/operations/storing-data.md/#using-local-cache)
- [S3へのバックアップ用`s3_plain`](/docs/ja/operations/backup#backuprestore-using-an-s3-disk)
- [S3の不変、非レプリカテーブル用`s3_plain_rewritable`](/docs/ja/operations/storing-data.md#s3-plain-rewritable-storage)

## データストレージに複数のブロックデバイスを使用する {#table_engine-mergetree-multiple-volumes}

### 概要 {#introduction}

`MergeTree`ファミリーのテーブルエンジンは、データを複数のブロックデバイスに保存することができます。たとえば、特定のテーブルのデータが暗黙的に"ホット"および"コールド"に分割される場合に便利です。最近のデータは定期的に要求されますが、必要なスペースは小さいです。反対に、歴史的データはまれに要求されることがあり、通常、多くのスペースを必要とします。複数のディスクが利用可能な場合、"ホット"データは高速ディスク（例えば、NVMe SSDやメモリ内）に配置し、"コールド"データは比較的遅いディスク（例えば、HDD）に配置することができます。

データパートは`MergeTree`-エンジンテーブルにおける最小移動単位です。1つのパートに属するデータは、1つのディスクに保存されます。データパーツはユーザーの設定に従ってバックグラウンドで移動することや、[ALTER](/docs/ja/sql-reference/statements/alter/partition.md/#alter_move-partition)クエリを使用して移動することができます。

### 用語 {#terms}

- ディスク — ファイルシステムにマウントされたブロックデバイス。
- デフォルトディスク — [path](/docs/ja/operations/server-configuration-parameters/settings.md/#path)サーバー設定で指定されたパスを保管するディスク。
- ボリューム — 等しいディスクの順序付きセット（[JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)に類似）。
- ストレージポリシー — ボリュームとそれらの間でデータを移動するためのルールのセット。

説明されたエンティティに与えられた名前は、システムテーブル[system.storage_policies](/docs/ja/operations/system-tables/storage_policies.md/#system_tables-storage_policies)および[system.disks](/docs/ja/operations/system-tables/disks.md/#system_tables-disks)にあります。テーブルに適用されたストレージポリシーを指定するために、`MergeTree`-エンジンファミリーテーブルの`storage_policy`設定を使用します。

### 設定 {#table_engine-mergetree-multiple-volumes_configure}

ディスク、ボリューム、およびストレージポリシーは、`<storage_configuration>`タグ内で、もしくは`config.d`ディレクトリ内のファイルで宣言されるべきです。

:::tip
ディスクはクエリの`SETTINGS`セクションでも宣言できます。これは例えばURLにホストされている一時的なディスクをアタッチするためのアドホックな分析に便利です。詳細については[動的ストレージ](#dynamic-storage)を参照してください。
:::

設定構造：

``` xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- ディスク名 -->
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

タグ：

- `<disk_name_N>` — ディスク名。全てのディスクで名前が異なる必要があります。
- `path` — サーバーがデータ（`data`および`shadow`フォルダー）を保存するパスは'/'で終わる必要があります。
- `keep_free_space_bytes` — 予約されるディスクの空き容量の量。

ディスク定義の順序は重要ではありません。

ストレージポリシー設定マークアップ：

``` xml
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
                    <!-- 設定 -->
                </volume_name_2>
                <!-- その他のボリューム -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- 設定 -->
        </policy_name_2>

        <!-- その他のポリシー -->
    </policies>
    ...
</storage_configuration>
```

タグ：

- `policy_name_N` — ポリシー名。ポリシー名はユニークでなければなりません。
- `volume_name_N` — ボリューム名。ボリューム名はユニークでなければなりません。
- `disk` — ボリューム内のディスク。
- `max_data_part_size_bytes` — ボリュームのいずれかのディスクに保存できるパートの最大サイズです。統合されるパートのサイズが`max_data_part_size_bytes`よりも大きい場合、このパートは次のボリュームに書き込まれます。基本的に、この機能により、新しい/小さなパーツをホット（SSD）ボリュームに保持し、それが大きなサイズに達するとコールド（HDD）ボリュームに移動させます。ポリシーが1つのボリュームしか持たない場合、この設定は使用しないでください。
- `move_factor` — 利用可能な空きスペースの量がこのファクター以下になると、データは自動的に次のボリュームに移動を開始します（デフォルトは0.1）。ClickHouseは既存のパートをサイズの大きい順番で並べ替え（降順で）`move_factor`条件を満たすのに十分な合計サイズのパートを選択します。すべてのパートの合計サイズが不十分な場合、すべてのパートが移動されます。
- `perform_ttl_move_on_insert` — データパート挿入時のTTL移動を無効にします。（デフォルトでは有効です）もし挿入されたデータパートがすでにTTL移動ルールによって期限切れだった場合、それは即座に移動ルールで宣言されたボリューム/ディスクに移動します。もし、この設定が無効になっている場合、すでに期限切れのデータパートはデフォルトボリュームに書き込まれ、その後にTTLボリュームに移動されます。
- `load_balancing` - ディスクバランシングのポリシー、`round_robin`または`least_used`。
- `least_used_ttl_ms` - すべてのディスクの利用可能なスペースを更新するためのタイムアウトを設定します（ミリ秒単位、`0` - 常に更新、`-1` - 一度も更新しない、デフォルトは`60000`）。ディスクはClickHouseによってのみ使用可能であり、オンラインファイルシステムのサイズ変更/縮小の対象ではない場合、それを`-1`に設定できますが、他のケースでは、誤ったスペース配分を引き起こす可能性があるため、お勧めできません。
- `prefer_not_to_merge` — この設定を使用しないでください。このボリュームでのデータパーツのマージを無効にします（この設定は有害でパフォーマンス低下をもたらします）。この設定が有効な場合（そうしないでください）、このボリュームでのデータマージは許可されません（これは悪いことです）。これにより（でも何かを制御しようとしているなら、間違っています）、ClickHouseが遅いディスクとうまく動作する方法を制御します（しかし、ClickHouseはそれをよく知っているので、この設定を使用しないでください）。
- `volume_priority` — ボリュームが埋められる順序（優先順位）を定義します。低い値は高い優先度を意味します。このパラメータの値は自然数であり、スキップ無しにNまでの範囲をカバーしている必要があります（最も低い優先順位が与えられたもの）。
  * すべてのボリュームがタグ付けされている場合、それらは指定された順序で優先されます。
  * 一部のボリュームのみがタグ付けされている場合、それらはタグのないボリュームの最低優先順位を持ち、それらは設定内で定義されている順序になります。
  * すべてのボリュームがタグ付けされていない場合、それらの優先度は設定で宣言されている順序に応じて設定されます。
  * 2つのボリュームが同じ優先度の値を持つことはできません。

設定例：

``` xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- ポリシー名 -->
            <volumes>
                <single> <!-- ボリューム名 -->
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

この例で、`hdd_in_order`ポリシーは[ラウンドロビン](https://en.wikipedia.org/wiki/Round-robin_scheduling)方式を実装しています。よって、このポリシーは1つのボリューム（`single`）のみを定義し、このボリュームのディスクには循環的にデータパーツが保存されます。このようなポリシーは、システムに複数の類似ディスクがマウントされているがRAIDが構成されていない場合に非常に便利です。列個のディスクドライブは信頼性が低いので、3以上のレプリケーションファクターでそれを補うことをお勧めします。

システムに異なる種類のディスクがある場合、代わりに`moving_from_ssd_to_hdd`ポリシーを使用することができます。ボリューム`hot`はSSDディスク（`fast_ssd`）から構成されており、その最大パートサイズは1GBです。1GBを超えるサイズのすべてのパートは、直接`cold`ボリューム（`disk1`がHDDディスク）に保存されます。
また、`fast_ssd`ディスクのスペースが80％以上で埋まると、データはバックグラウンドプロセスによって`disk1`に転送されます。
ストレージポリシー内でのボリューム列挙の順序は、少なくとも一つのボリュームに明示的な`volume_priority`パラメータが設定されていない場合、重要です。あるボリュームが満杯になると、データは次のボリュームへ移動します。ディスク列挙の順序も重要であり、データは順番に記憶されます。

テーブルを作成する際、設定済みのストレージポリシーの一つを適用できます:

``` sql
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

`default`ストレージポリシーは一つのディスクのみを用いる一つのボリュームを使用することを意味します。テーブル作成後にストレージポリシーを[ALTER TABLE ... MODIFY SETTING]クエリで変更可能で、新ポリシーには同じ名前の古いディスクとボリュームを全て含める必要があります。

データパーツのバックグラウンド移動を行うスレッド数は、[background_move_pool_size](/docs/ja/operations/server-configuration-parameters/settings.md/#background_move_pool_size)設定で変更できます。

### 詳細 {#details}

`MergeTree`テーブルの場合、データは異なる方法でディスクに保存されます：

- インサートの結果として（`INSERT`クエリ）。
- バックグラウンドでのマージや[変異](/docs/ja/sql-reference/statements/alter/index.md#alter-mutations)の間。
- 他のレプリカからのダウンロード時。
- パーティションの凍結の結果として[ALTER TABLE ... FREEZE PARTITION](/docs/ja/sql-reference/statements/alter/partition.md/#alter_freeze-partition)。

変異とパーティションの凍結を除く他のケースでは、パーツは指定されたストレージポリシーに従ってボリュームおよびディスク上に保存されます：

1.  ディスクスペースが十分で(`unreserved_space > current_part_size`)かつ指定サイズのパーツを保存できる(`max_data_part_size_bytes > current_part_size`)最初のボリュームが選ばれます。
2.  このボリューム内では、以前保存したデータチャンクに続くディスクかつパーツサイズより自由スペースが大きい(`unreserved_space - keep_free_space_bytes > current_part_size`)ディスクが選ばれます。

内部では、変異やパーティションの凍結は[ハードリンク](https://en.wikipedia.org/wiki/Hard_link)を使用します。異なるディスク間のハードリンクはサポートされていないため、そのような場合、結果として得られるパーツは最初のものと同じディスクに保存されます。

バックグラウンドで、パーツは設定ファイルに宣言された順序に基づき、自由スペースの量(`move_factor`パラメータ)に基づいてボリューム間で移動されます。データは最後のボリュームから最初のボリュームには決して移されません。システムテーブル[system.part_log](/docs/ja/operations/system-tables/part_log.md/#system_tables-part-log)（フィールド`type = MOVE_PART`）および[system.parts](/docs/ja/operations/system-tables/parts.md/#system_tables-parts)（フィールド`path`及び`disk`）を使用してバックグラウンドの移動を監視することができます。詳細な情報はサーバーログにも記録されます。

ユーザーはクエリ[ALTER TABLE ... MOVE PART\|PARTITION ... TO VOLUME\|DISK ...](/docs/ja/sql-reference/statements/alter/partition.md/#alter_move-partition)を使用して、ボリュームからボリュームへ、あるいはディスクへのパーツやパーティションの移動を強制することができます。バックグラウンド操作の制限が考慮されます。このクエリは自身で移動を開始し、バックグラウンド操作の完了を待ちません。十分な空きスペースがないか、必要な条件が満たされない場合はエラーメッセージが表示されます。

データの移動はデータのレプリケーションに干渉しません。したがって、異なるストレージポリシーを同じテーブルの異なるレプリカに指定できます。

バックグラウンドのマージや変異が完了した後、古いパーツは一定期間(`old_parts_lifetime`)を経てからのみ削除されます。この時間内、これらは他のボリュームやディスクには移動されません。そのため、パーツが最終的に削除されるまで、それらは依然としてディスクスペースの評価に考慮されます。

ユーザーは、[JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)ボリュームの異なるディスクに対して、新しい大きなパーツをバランス良く割り当てることができ、[min_bytes_to_rebalance_partition_over_jbod](/docs/ja/operations/settings/merge-tree-settings.md/#min-bytes-to-rebalance-partition-over-jbod)設定を使用します。

## 外部ストレージを用いたデータストレージの使用 {#table_engine-mergetree-s3}

[MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md)ファミリーテーブルエンジンは、タイプ` s3`、` azure_blob_storage`、` hdfs`を持つディスクを使用してデータを`S3`、`AzureBlobStorage`、`HDFS`に保存できます。詳細は[外部ストレージオプションの設定](/docs/ja/operations/storing-data.md/#configuring-external-storage)を参照してください。

外部ストレージとして`S3`を使用する場合の例：

設定マークアップ：
``` xml
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

また[外部ストレージオプションの設定](/docs/ja/operations/storing-data.md/#configuring-external-storage)も参照してください。

:::note cache configuration
ClickHouse バージョン 22.3 から 22.7 は異なるキャッシュ設定を使用しています。これらのバージョンを使用している場合は[ローカルキャッシュの使用](/docs/ja/operations/storing-data.md/#using-local-cache)を参照してください。
:::

## 仮想カラム {#virtual-columns}

- `_part` — パーツの名前。
- `_part_index` — クエリ結果でのパーツの連続インデックス。
- `_partition_id` — パーティションの名前。
- `_part_uuid` — 一意のパーツ識別子（MergeTree設定`assign_part_uuids`が有効な場合）。
- `_partition_value` — `partition by`式の値（タプル）。
- `_sample_factor` — サンプルファクター（クエリから）。
- `_block_number` — 行のブロック番号。`allow_experimental_block_number_column`が真に設定されている場合にマージで保持されます。

## カラム統計 (エクスペリメンタル) {#column-statistics}

統計の宣言は`set allow_experimental_statistics = 1`を有効にした際、`*MergeTree*`ファミリーのテーブルに対する`CREATE`クエリのカラムセクション内にあります。

``` sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

統計は`ALTER`文を使って操作することもできます。

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

これらの軽量な統計はカラム内の値の分布に関する情報を集約します。統計は各パーツに保存され、各インサートが行われるたびに更新されます。
`set allow_statistics_optimize = 1`を有効にすれば、プレウェア最適化にのみ使用されます。

### カラム統計の利用可能なタイプ {#available-types-of-column-statistics}

- `MinMax`

    数値カラムの範囲フィルターの選択性を推定するために使用される最小および最大カラム値。

    構文: `minmax`

- `TDigest`

    数値カラムに対しておおよその百分位数（例：90パーセンタイル）を計算するための[TDigest](https://github.com/tdunning/t-digest)スケッチ。

    構文: `tdigest`

- `Uniq`

    カラムが含む異なる値の数を推定するための[HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog)スケッチ。

    構文: `uniq`

- `CountMin`

    カラム内の各値の頻度をおおよそカウントする[CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch)スケッチ。

    構文: `countmin`

### サポートされるデータ型 {#supported-data-types}

|           | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | String or FixedString |
|-----------|----------------------------------------------------|-----------------------|
| CountMin  | ✔                                                  | ✔                     |
| MinMax    | ✔                                                  | ✗                     |
| TDigest   | ✔                                                  | ✗                     |
| Uniq      | ✔                                                  | ✔                     |

### サポートされる操作 {#supported-operations}

|           | Equality filters (==) | Range filters (>, >=, <, <=) |
|-----------|-----------------------|------------------------------|
| CountMin  | ✔                     | ✗                            |
| MinMax    | ✗                     | ✔                            |
| TDigest   | ✗                     | ✔                            |
| Uniq      | ✔                     | ✗                            |

## カラムレベルの設定 {#column-level-settings}

特定のMergeTree設定はカラムレベルでオーバーライド可能です：

- `max_compress_block_size` — テーブルへの書き込み時に圧縮される前の非圧縮データブロックの最大サイズ。
- `min_compress_block_size` — 次のマークの書き込み時に圧縮が必要な非圧縮データブロックの最小サイズ。

例：

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

カラムレベルの設定は[ALTER MODIFY COLUMN](/docs/ja/sql-reference/statements/alter/column.md)を使用して修正または削除できます、例：

- カラム宣言から`SETTINGS`を削除：

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

- 設定を修正：

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

- 一つまたは複数の設定をリセットし、テーブルの`CREATE`クエリでのカラム表現内の設定宣言も削除：

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```
