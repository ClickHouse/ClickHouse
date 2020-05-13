---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 35
toc_title: CREATE
---

# クエリの作成 {#create-queries}

## CREATE DATABASE {#query-language-create-database}

データベースの作成。

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### 句 {#clauses}

-   `IF NOT EXISTS`

        If the `db_name` database already exists, then ClickHouse doesn't create a new database and:

        - Doesn't throw an exception if clause is specified.
        - Throws an exception if clause isn't specified.

-   `ON CLUSTER`

        ClickHouse creates the `db_name` database on all the servers of a specified cluster.

-   `ENGINE`

        - [MySQL](../engines/database_engines/mysql.md)

            Allows you to retrieve data from the remote MySQL server.

        By default, ClickHouse uses its own [database engine](../engines/database_engines/index.md).

## CREATE TABLE {#create-table-query}

その `CREATE TABLE` クエリには複数の形式を使用できます。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

名前の付いた表を作成します ‘name’ で ‘db’ データベースまたは ‘db’ は設定されていない。 ‘engine’ エンジン。
テーブルの構造は、列の説明のリストです。 た場合の指数については、エンジンとして表示していパラメータテーブルのエンジンです。

列の説明は次のとおりです `name type` 最も単純なケースでは。 例えば: `RegionID UInt32`.
デフォルト値に対して式を定義することもできます(下記参照)。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

別のテーブルと同じ構造のテーブルを作成します。 テーブルに別のエンジンを指定できます。 エンジンが指定されていない場合は、同じエンジンが `db2.name2` テーブル。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

テーブルを作成しますの構造やデータによって返される [テーブル機能](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

の結果のような構造を持つテーブルを作成します。 `SELECT` クエリ、 ‘engine’ エンジンは、SELECTからのデータでそれを埋めます。

すべての場合において、 `IF NOT EXISTS` テーブルが既に存在する場合、クエリはエラーを返しません。 この場合、クエリは何もしません。

後に他の節がある場合もあります `ENGINE` クエリ内の句。 テーブルの作成方法に関する詳細なドキュメントを参照してください [表エンジン](../../engines/table-engines/index.md#table_engines).

### デフォルト値 {#create-default-values}

列の説明では、次のいずれかの方法で、既定値の式を指定できます:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
例えば: `URLDomain String DEFAULT domain(URL)`.

デフォルト値の式が定義されていない場合、デフォルト値は数値の場合はゼロに、文字列の場合は空の文字列に、配列の場合は空の配列に設定され `0000-00-00` 日付または `0000-00-00 00:00:00` 時間の日付のため。 Nullはサポートされていません。

既定の式が定義されている場合、列の型は省略可能です。 明示的に定義された型がない場合は、既定の式の型が使用されます。 例えば: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ タイプは ‘EventDate’ コラム

データ型と既定の式が明示的に定義されている場合、この式は型キャスト関数を使用して指定された型にキャストされます。 例えば: `Hits UInt32 DEFAULT 0` と同じことを意味します `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don’t contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

通常のデフォルト値。 insertクエリで対応する列が指定されていない場合は、対応する式を計算して入力します。

`MATERIALIZED expr`

マテリアライズド式。 このような列は、常に計算されるため、insertに指定することはできません。
列のリストのないinsertの場合、これらの列は考慮されません。
また、selectクエリでアスタリスクを使用する場合、この列は置換されません。 これは、ダンプが以下を使用して取得した不変量を保持するためです `SELECT *` 列のリストを指定せずにINSERTを使用してテーブルに戻すことができます。

`ALIAS expr`

同義語。 このような列は、テーブルにはまったく格納されません。
その値はテーブルに挿入することはできず、selectクエリでアスタリスクを使用するときは置換されません。
クエリの解析中にエイリアスが展開されている場合は、selectで使用できます。

ALTER queryを使用して新しい列を追加する場合、これらの列の古いデータは書き込まれません。 代わりに、新しい列の値を持たない古いデータを読み取る場合、式は既定でオンザフライで計算されます。 ただし、式を実行するために、クエリで指定されていない異なる列が必要な場合、これらの列は追加で読み取られますが、必要なデータブロックに対し

新しい列をテーブルに追加し、後でそのデフォルトの式を変更すると、古いデータに使用される値が変更されます（ディスクに値が格納されていないデー バックグラウンドマージを実行すると、マージパーツのいずれかにない列のデータがマージされたパーツに書き込まれます。

入れ子になったデータ構造の要素の既定値を設定することはできません。

### 制約 {#constraints}

列と共に、説明の制約を定義することができます:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` 任意のブール式でできます。 場合に制約の定義のテーブルのそれぞれチェック毎に行 `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

追加大量の制約になる可能性の性能を大 `INSERT` クエリ。

### TTL式 {#ttl-expression}

値の保存時間を定義します。 mergetree-familyテーブルにのみ指定できます。 詳細な説明については、 [列とテーブルのttl](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### 列圧縮コーデック {#codecs}

デフォルトでは、clickhouseは `lz4` 圧縮方法。 のために `MergeTree`-エンジンファミリでは、デフォルトの圧縮方法を変更できます [圧縮](../../operations/server-configuration-parameters/settings.md#server-settings-compression) サーバー構成のセクション。 また、各列の圧縮方法を定義することもできます。 `CREATE TABLE` クエリ。

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

コーデックが指定されている場合、既定のコーデックは適用されません。 コーデックの組合せでのパイプライン、例えば, `CODEC(Delta, ZSTD)`. の選定と大型ブリッジダイオードコーデックの組み合わせますプロジェクト、ベンチマークと同様に記載のAltinity [ClickHouseの効率を改善する新しいエンコーディング](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) 記事。

!!! warning "警告"
    できない解凍clickhouseデータベースファイルを外部の事のように `lz4`. 代わりに、特別な [clickhouse-コンプレッサー](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) 効用だ

圧縮できるようになりました以下のテーブルエンジン:

-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家族 列圧縮コーデックをサポートし、既定の圧縮方法を選択する [圧縮](../../operations/server-configuration-parameters/settings.md#server-settings-compression) 設定。
-   [ログ](../../engines/table-engines/log-family/log-family.md) 家族 使用します `lz4` 圧縮メソッドはデフォルト対応カラムの圧縮コーデック.
-   [セット](../../engines/table-engines/special/set.md). 唯一のデフォルトの圧縮をサポート。
-   [参加](../../engines/table-engines/special/join.md). 唯一のデフォルトの圧縮をサポート。

ClickHouse支援共通の目的コーデックや専門のコーデック.

#### 特殊コーデック {#create-query-specialized-codecs}

これらのコーデックしていただくための圧縮により効果的な利用の特徴データです。 これらのコーデックの一部は、データ自身を圧縮しない。 その代わりに、それらのデータを共通の目的コーデックは、圧縮です。

特殊コーデック:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` デルタ値を格納するために使用されます。 `delta_bytes` raw値の最大サイズです。 可能 `delta_bytes` 値:1,2,4,8. のデフォルト値 `delta_bytes` は `sizeof(type)` 1、2、4、または8に等しい場合。 それ以外の場合は1です。
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla:高速でスケーラブルなメモリ内の時系列データベース](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorilla:高速でスケーラブルなメモリ内の時系列データベース](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` と `DateTime`). アルゴリズムの各ステップで、codecは64値のブロックを取り、64x64ビット行列にそれらを入れ、それを転置し、未使用の値をトリミングし、残りをシーケ 未使用のビットは、圧縮が使用されるデータ部分全体の最大値と最小値の間で異ならないビットです。

`DoubleDelta` と `Gorilla` コーデックは、その圧縮アルゴリズムの構成要素としてゴリラTSDBで使用されています。 Gorillaのアプローチは、タイムスタンプで徐々に変化する値のシーケンスがある場合のシナリオで有効です。 タイムスタンプは、 `DoubleDelta` コーデックおよび値はによって効果的に圧縮されます `Gorilla` コーデック。 たとえば、効果的に格納されたテーブルを取得するには、次の構成でテーブルを作成します:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### 一般的な目的のコーデック {#create-query-common-purpose-codecs}

コーデック:

-   `NONE` — No compression.
-   `LZ4` — Lossless [データ圧縮](https://github.com/lz4/lz4) 既定で使用されます。 LZ4高速圧縮を適用します。
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` 既定のレベルを適用します。 可能なレベル：\[1、12\]。 推奨レベル範囲：\[4、9\]。
-   `ZSTD[(level)]` — [ZSTD圧縮アルゴリズム](https://en.wikipedia.org/wiki/Zstandard) 構成可能を使って `level`. 可能なレベル：\[1、22\]。 デフォルト値:1。

圧縮レベルが高い場合は、圧縮回数、繰り返しの解凍などの非対称シナリオに役立ちます。 高いレベルは、より良い圧縮と高いcpu使用率を意味します。

## 一時テーブル {#temporary-tables}

ClickHouseは次の特徴がある一時テーブルを支える:

-   一時テーブルは、接続が失われた場合など、セッションが終了すると消えます。
-   一時テーブルはメモリエンジンのみを使用します。
-   一時テーブルにdbを指定することはできません。 データベースの外部で作成されます。
-   すべてのクラスタサーバー上に分散ddlクエリを使用して一時テーブルを作成することは不可能です `ON CLUSTER`):このテーブルは現在のセッションにのみ存在します。
-   テンポラリテーブルの名前が別のテーブルと同じ場合、クエリでdbを指定せずにテーブル名を指定すると、テンポラリテーブルが使用されます。
-   分散クエリ処理では、クエリで使用される一時テーブルがリモートサーバーに渡されます。

一時テーブルを作成するには、次の構文を使用します:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

ほとんどの場合、一時テーブルを手動で作成され、外部データを利用するためのクエリに対して、または配布 `(GLOBAL) IN`. 詳細は、該当するセクションを参照してください

テーブルを使用することは可能です [エンジン=メモリ](../../engines/table-engines/special/memory.md) 一時テーブルの代わりに。

## 分散ddlクエリ(on cluster clause) {#distributed-ddl-queries-on-cluster-clause}

その `CREATE`, `DROP`, `ALTER`、と `RENAME` クエリの支援の分散実行クラスター
たとえば、次のクエリを作成します `all_hits` `Distributed` 各ホストのテーブル `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

これらのクエリを正しく実行するには、各ホストが同じクラスタ定義を持っている必要があります（設定の同期を簡単にするために、zookeeperからの置換 彼らはまた、zookeeperサーバに接続する必要があります。
クエリのローカルバージョンは、一部のホストが現在利用できない場合でも、最終的にクラスター内の各ホストに実装されます。 単一のホスト内でクエリを実行する順序は保証されます。

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

ビューを作成します。 通常とマテリアライズド：ビューの二つのタイプがあります。

通常のビューにはデータは保存されませんが、別のテーブルから読み取るだけです。 言い換えれば、通常のビューは、保存されたクエリに過ぎません。 ビューから読み取る場合、この保存されたクエリはfrom句のサブクエリとして使用されます。

たとえば、ビューを作成したとします:

``` sql
CREATE VIEW view AS SELECT ...
```

とクエリを書かれた:

``` sql
SELECT a, b, c FROM view
```

このクエリは、サブクエリの使用と完全に同じです:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

実現の景色でデータ変換に対応する選択を返します。

マテリアライズドビューを作成するとき `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

マテリアライズドビューを作成するとき `TO [db].[table]`、使用してはならない `POPULATE`.

SELECTで指定されたテーブルにデータを挿入すると、挿入されたデータの一部がこのSELECTクエリによって変換され、結果がビューに挿入されます。

POPULATEを指定すると、作成時に既存のテーブルデータがビューに挿入されます。 `CREATE TABLE ... AS SELECT ...` . そうしないと、クエリーを含み、データを挿入し、表の作成後、作成した。 ビューの作成時にテーブルに挿入されたデータは挿入されないため、POPULATEを使用することはお勧めしません。

A `SELECT` クエ `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` が設定され、データは挿入中に集約されるが、挿入されたデータの単一パケット内にのみ存在する。 データはそれ以上集計されません。 例外は、次のようなデータの集計を個別に実行するエンジンを使用する場合です `SummingMergeTree`.

の実行 `ALTER` クエリを実現眺めなが十分に整備されていないので、いかに不便です。 マテリアライズドビュ `TO [db.]name`、できます `DETACH` ビュー、実行 `ALTER` ターゲットテーブルの場合 `ATTACH` 以前に切り離さ (`DETACH`)ビュー。

ビューの外観は、通常のテーブルと同じです。 例えば、それらはの結果にリストされています `SHOW TABLES` クエリ。

ビューを削除するための別のクエリはありません。 ビューを削除するには `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

作成 [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) 与えられると [構造](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [ソース](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [レイアウト](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) と [寿命](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

外部辞書構造の属性です。 ディクショナリ属性は、表の列と同様に指定します。 唯一の必須の属性は、そのタイプ、その他すべてのプロパティがデフォルト値がある。

辞書に応じて [レイアウト](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) 一つ以上の属性は、辞書キーとして指定することができます。

詳細については、 [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) セクション。

[元の記事](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
