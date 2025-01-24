---
slug: /ja/sql-reference/statements/create/table
sidebar_position: 36
sidebar_label: TABLE
title: "CREATE TABLE"
keywords: [compression, codec, schema, DDL]
---

新しいテーブルを作成します。このクエリは、使用ケースに応じてさまざまな構文形式を持つことができます。

デフォルトでは、テーブルは現在のサーバーのみに作成されます。分散DDLクエリは `ON CLUSTER` 句として実装されており、[別途説明されています](../../../sql-reference/distributed-ddl.md)。

## 構文形式

### 明示的なスキーマを使用する場合

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'カラムのコメント'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'カラムのコメント'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'テーブルのコメント']
```

`table_name` という名前のテーブルを `db` データベース、または `db` が設定されていない場合は現在のデータベースに作成し、括弧内に指定された構造と `engine` エンジンで作成します。
テーブルの構造はカラムの記述、二次インデックス、制約のリストです。エンジンが[主キー](#primary-key)をサポートしている場合、テーブルエンジンのパラメータとして示されます。

カラムの記述は、最も単純な場合には `name type` です。例：`RegionID UInt32`。

デフォルト値のための式も定義できます（以下を参照）。

必要に応じて、一つまたは複数のキー式を指定して主キーを指定できます。

カラムとテーブルにコメントを追加できます。

### 別のテーブルに類似したスキーマを使用する場合

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

別のテーブルと同じ構造のテーブルを作成します。テーブルに異なるエンジンを指定できます。エンジンが指定されていない場合、`db2.name2` テーブルと同じエンジンが使用されます。

### 別のテーブルからスキーマとデータをクローンする場合

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name CLONE AS [db2.]name2 [ENGINE = engine]
```

別のテーブルと同じ構造のテーブルを作成します。テーブルに異なるエンジンを指定できます。エンジンが指定されていない場合、`db2.name2` テーブルと同じエンジンが使用されます。新しいテーブルが作成された後、`db2.name2` からのすべてのパーティションがそれに添付されます。言い換えれば、`db2.name2` のデータは作成時に `db.table_name` にクローンされます。このクエリは次のものと同等です：

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine];
ALTER TABLE [db.]table_name ATTACH PARTITION ALL FROM [db2].name2;
```

### テーブル関数から作成する場合

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

指定された[テーブル関数](../../../sql-reference/table-functions/index.md#table-functions)と同じ結果のテーブルを作成します。作成されたテーブルは、指定された対応するテーブル関数と同様に機能します。

### SELECT クエリから作成する場合

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

`SELECT` クエリの結果に似た構造で、データを `SELECT` から取得して `engine` エンジンでテーブルを作成します。また、カラムの説明を明示的に指定できます。

テーブルがすでに存在し、`IF NOT EXISTS` が指定されている場合、クエリは何も行いません。

クエリ内の `ENGINE` 句の後に他の句を指定することができます。テーブル作成の詳細なドキュメントについては、[テーブルエンジン](../../../engines/table-engines/index.md#table_engines)の説明を参照してください。

:::tip
ClickHouse Cloud では、以下の2つのステップに分けてください：
1. テーブル構造の作成

  ```sql
  CREATE TABLE t1
  ENGINE = MergeTree
  ORDER BY ...
  # highlight-next-line
  EMPTY AS
  SELECT ...
  ```

2. テーブルへのデータ投入

  ```sql
  INSERT INTO t1
  SELECT ...
  ```

:::

**例**

クエリ:

``` sql
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

結果:

```text
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

## NULL または NOT NULL 修飾子

カラム定義後のデータ型に続く `NULL` および `NOT NULL` 修飾子 は、それが [Nullable](../../../sql-reference/data-types/nullable.md#data_type-nullable) を許可するかどうかを示します。

型が `Nullable` でない場合、`NULL` が指定されるとそれは `Nullable` として扱われ、`NOT NULL` が指定されるとそうではありません。例えば、`INT NULL` は `Nullable(INT)` と同じです。型が `Nullable` で、`NULL` または `NOT NULL` 修飾子が指定されている場合、例外が発生します。

また、[data_type_default_nullable](../../../operations/settings/settings.md#data_type_default_nullable) 設定も参照してください。

## デフォルト値 {#default_values}

カラムの記述は `DEFAULT expr`、`MATERIALIZED expr`、または `ALIAS expr` の形式でデフォルト値の式を指定できます。例: `URLDomain String DEFAULT domain(URL)`。

式 `expr` は省略可能です。省略された場合、カラムの型は明示的に指定する必要があり、デフォルト値は数値カラムの場合は `0`、文字列カラムの場合は `''`（空の文字列）、配列カラムの場合は `[]`（空の配列）、日付カラムの場合は `1970-01-01`、Nullable カラムの場合は `NULL` です。

デフォルト値カラムの型は省略可能であり、その場合は `expr` の型から推測されます。例えば、カラム `EventDate DEFAULT toDate(EventTime)` は日付型になります。

データ型とデフォルト値の式の両方が指定されている場合、式を指定された型に変換する暗黙の型キャスト関数が挿入されます。例: `Hits UInt32 DEFAULT 0` は内部的には `Hits UInt32 DEFAULT toUInt32(0)` として表現されます。

デフォルト値の式 `expr` は任意のテーブルカラムや定数を参照できます。ClickHouse は、テーブル構造の変更が式の計算にループを導入しないことを確認します。INSERTの際には、式が解決可能であること、すなわち計算に使われるすべてのカラムが渡されていることを確認します。

### DEFAULT

`DEFAULT expr`

通常のデフォルト値。このようなカラムの値がINSERTクエリで指定されていない場合、`expr` から計算されます。

例:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) Values (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

### MATERIALIZED

`MATERIALIZED expr`

マテリアライズド式。このようなカラムの値は行が挿入されるときに指定されたマテリアライズド式に従って自動的に計算されます。値は `INSERT` の際に明示的に指定することはできません。

また、このタイプのデフォルト値カラムは `SELECT *` の結果には含まれません。これは、`SELECT *` の結果を常に `INSERT` を使用してテーブルに再挿入できるという不変性を維持するためです。この動作は `asterisk_include_materialized_columns` 設定を使って無効にできます。

例:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test Values (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

### EPHEMERAL

`EPHEMERAL [expr]`

エフェメラルカラム。このタイプのカラムはテーブルに保存されず、SELECTすることもできません。エフェメラルカラムの唯一の目的は、他のカラムのデフォルト値式をそれから構築することです。

明示的に指定されていないカラムなしでの挿入は、このタイプのカラムをスキップします。これは、`SELECT *` の結果を常に `INSERT` を使用してテーブルに再挿入できるという不変性を維持するためです。

例:

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) Values (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

### ALIAS

`ALIAS expr`

計算カラム（別名）。このタイプのカラムはテーブルに保存されず、INSERT値を挿入することもできません。

SELECTクエリがこのタイプのカラムを明示的に参照すると、値は `expr` からクエリ時に計算されます。デフォルトでは、`SELECT *` はALIASカラムを除外します。この動作は設定 `asterisk_include_alias_columns` で無効にすることができます。

ALTERクエリを使用して新しいカラムを追加すると、これらのカラムの古いデータは書き込まれません。代わりに、古いデータを読み取るときに、新しいカラムの値がディスク上に保存されていない場合、デフォルトで式がオンザフライで計算されます。ただし、式の実行に異なるカラムが必要で、クエリに示されていない場合、必要なデータブロックに対してだけ追加でこれらのカラムが読み取られます。

新しいカラムをテーブルに追加しても、デフォルト式を後で変更した場合、古いデータに使用される値は変更されます（ディスク上に値が保存されていなかったデータに対して）。バックグラウンドでのマージの実行時には、結合されている部分の1つで値が欠けているカラムのデータが結合された部分に書き込まれます。

ネストされたデータ構造の要素にデフォルト値を設定することはできません。

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

## 主キー

テーブルを作成するときに[主キー](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)を定義できます。主キーは2つの方法で指定できます:

- カラムリスト内

``` sql
CREATE TABLE db.table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

- カラムリスト外

``` sql
CREATE TABLE db.table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
1つのクエリで両方の方法を組み合わせることはできません。
:::

## 制約

カラムの記述とともに制約を定義することができます:

### CONSTRAINT

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` は任意のブール式にできます。テーブルに制約が定義されている場合、各行の `INSERT` クエリごとにそれらがチェックされます。どの制約も満たされない場合、サーバーは制約名とチェック対象の式で例外を発生させます。

大量の制約を追加すると、大規模な `INSERT` クエリのパフォーマンスに影響を与える可能性があります。

### ASSUME

`ASSUME` 句は、テーブル上に常に真であると仮定する `CONSTRAINT` を定義するために使用されます。この制約は、SQLクエリのパフォーマンスを向上させるためにオプティマイザーによって使用されます。

この例では、`ASSUME CONSTRAINT` が `users_a` テーブルの作成で使用されています:

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

ここで、`ASSUME CONSTRAINT` は、`length(name)` 関数が常に `name_len` カラムの値と等しいことを保証するために使用されています。つまり、`length(name)` がクエリで呼び出されるたびに、ClickHouseはそれを `name_len` で置き換えることができるため、`length()` 関数を呼び出すことを避けるために高速になります。

次に、クエリ `SELECT name FROM users_a WHERE length(name) < 5;` を実行すると、ClickHouseは`ASSUME CONSTRAINT` によって `SELECT name FROM users_a WHERE name_len < 5;` に最適化します。これにより、各行の `name` の長さを計算することを避けるため、クエリが高速になります。

`ASSUME CONSTRAINT` は制約を強制するものではなく、単にオプティマイザーに制約が真であることを知らせるものです。制約が実際に真でない場合、クエリの結果が不正確になる可能性があります。したがって、制約が真であると確信している場合にのみ `ASSUME CONSTRAINT` を使用するべきです。

## 有効期限 (TTL) 式

値の保存期間を定義します。MergeTreeファミリーのテーブルでのみ指定可能です。詳細な説明については、[カラムとテーブルのTTL](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)を参照してください。

## カラム圧縮コーデック {#column_compression_codec}

デフォルトでは、セルフマネージドのClickHouseでは `lz4` 圧縮を適用し、ClickHouse Cloudでは `zstd` を適用します。

`MergeTree`エンジンファミリーの場合、サーバ構成の[compression](../../../operations/server-configuration-parameters/settings.md#server-settings-compression)セクションで、デフォルトの圧縮方法を変更することができます。

また、`CREATE TABLE` クエリ内で各カラムの圧縮方法を定義することもできます。

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

デフォルト圧縮を参照するには `Default` コーデックを指定できます。実行時に異なる設定（およびデータのプロパティ）に依存する可能性があります。
例: `value UInt64 CODEC(Default)` — コーデック指定がない場合と同じです。

また、カラムから現在のCODECを削除し、config.xmlからのデフォルト圧縮を使用することもできます：

``` sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

コードをパイプラインで組み合わせることもできます。例: `CODEC(Delta, Default)`。

:::tip
ClickHouseデータベースファイルを外部ユーティリティである `lz4` などで解凍することはできません。代わりに、特別な [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) ユーティリティを使用してください。
:::

圧縮がサポートされているテーブルエンジン:

- [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) ファミリー。カラム圧縮コーデックをサポートしており、圧縮設定でデフォルト圧縮方法を選択できます。
- [Log](../../../engines/table-engines/log-family/index.md) ファミリー。デフォルトで `lz4` 圧縮方法を使用し、カラム圧縮コーデックをサポートします。
- [Set](../../../engines/table-engines/special/set.md)。デフォルト圧縮のみサポートされています。
- [Join](../../../engines/table-engines/special/join.md)。デフォルト圧縮のみサポートされています。

ClickHouseは汎用のコーデックと専用のコーデックをサポートしています。

### 汎用コーデック

#### NONE

`NONE` — 圧縮なし。

#### LZ4

`LZ4` — デフォルトで使用されるロスレスな [データ圧縮アルゴリズム](https://github.com/lz4/lz4) です。LZ4の高速圧縮を適用します。

#### LZ4HC

`LZ4HC[(level)]` — LZ4 HC（高圧縮）アルゴリズムで、levelを設定可能です。デフォルトレベル: 9。`level <= 0` はデフォルトレベルを適用します。可能なレベル: \[1, 12\]。推奨レベル範囲: \[4, 9\]。

#### ZSTD

`ZSTD[(level)]` — [ZSTD 圧縮アルゴリズム](https://en.wikipedia.org/wiki/Zstandard) で、`level` を設定可能です。可能なレベル: \[1, 22\]。デフォルトレベル: 1。

高圧縮レベルは、非対称シナリオ、たとえば一度圧縮して何度も解凍する場合に有効です。高レベルはより良い圧縮と高いCPU使用率を意味します。

#### ZSTD_QAT

`ZSTD_QAT[(level)]` — [ZSTD 圧縮アルゴリズム](https://en.wikipedia.org/wiki/Zstandard) で、levelを設定可能、[Intel® QATlib](https://github.com/intel/qatlib) と [Intel® QAT ZSTD Plugin](https://github.com/intel/QAT-ZSTD-Plugin) によって実装されています。可能なレベル: \[1, 12\]。デフォルトレベル: 1。推奨レベル範囲: \[6, 12\]。いくつかの制限があります:

- ZSTD_QAT はデフォルトで無効になっており、設定 [enable_zstd_qat_codec](../../../operations/settings/settings.md#enable_zstd_qat_codec) を有効にした後にのみ使用できます。
- 圧縮するために、ZSTD_QAT は Intel® QAT オフロードデバイス ([QuickAssist Technology](https://www.intel.com/content/www/us/en/developer/topic-technology/open/quick-assist-technology/overview.html)) を使用しようとします。そのようなデバイスが見つからない場合、ソフトウェアでのZSTD圧縮にフォールバックします。
- 解凍は常にソフトウェアで行われます。

:::note
ZSTD_QAT は ClickHouse Cloud では利用できません。
:::

### 専用コーデック

これらのコーデックは、データの特定の特徴を活用して圧縮をより効果的にするために設計されています。これらのコーデックの中には、データ自体を圧縮するものではなく、データを準備するためのもので、次の一般目的コーデックを使った圧縮率を高めることを目的としています。

#### Delta

`Delta(delta_bytes)` — 元の値を隣接する2つの値の差に置換する圧縮方式。ただし、最初の値はそのままです。最大 `delta_bytes` がデルタ値を保存するために使用され、したがって `delta_bytes` は元の値の最大サイズです。可能な `delta_bytes` の値: 1, 2, 4, 8。デフォルトの `delta_bytes` 値は、1, 2, 4, または 8 のいずれかである場合は `sizeof(type)` 、その他の場合は 1 です。Delta はデータ準備コーデックであり、単独では使用できません。

#### DoubleDelta

`DoubleDelta(bytes_size)` — 二重のデルタを計算し、コンパクトなバイナリ形式で書き込みます。可能な `bytes_size` の値: 1, 2, 4, 8。デフォルトは、1, 2, 4, または 8 のいずれかの場合は `sizeof(type)` です。その他の場合は 1 です。一定の間隔を持つ単調増加のシーケンス、例えば時系列データで最適な圧縮率が達成されます。あらゆる固定幅型で使用可能。Gorilla TSDBで使用されるアルゴリズムを実装しており、これを64ビット型に拡張しています。32ビットのデルタには1ビット余分に使用されます：5ビットプレフィックス（4ビットプレフィックスではなく）。詳細は[Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf)の「タイムスタンプの圧縮」を参照してください。DoubleDelta はデータ準備コーデックであり、単独では使用できません。

#### GCD

`GCD()` — カラム内の値の最大公約数 (GCD) を計算し、各値をGCDで除算します。整数型、小数型、日付/時間型のカラムで使用できます。このコーデックはGCDの倍数で変化（増加または減少）する値を持つカラムに適しています。例：24, 28, 16, 24, 8, 24（GCD = 4）。GCDはデータ準備コーデックであり、単独では使用できません。

#### Gorilla

`Gorilla(bytes_size)` — 現在の浮動小数点値と前の値の間のXORを計算し、それをコンパクトなバイナリ形式で書き込みます。連続した値の違いが小さく（すなわち、シリーズの値がゆっくりと変化する場合）、圧縮率が向上します。Gorilla TSDBで使用されるアルゴリズムを実装しており、これを64ビット型に拡張しています。可能な `bytes_size` の値: 1, 2, 4, 8。デフォルトは、1, 2, 4, または 8 のいずれかの場合は `sizeof(type)` です。その他の場合は 1 です。さらなる情報については、[Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078)のセクション4.1を参照してください。

#### FPC

`FPC(level, float_size)` - 2つの予測子のうちより優れた方を使ってシーケンスの次の浮動小数点値を繰り返し予測し、実際の値と予測された値をXORし、その結果を先行ゼロ圧縮します。Gorillaに似ており、浮動小数点値のシリーズをゆっくりと変化させる際に効率的です。64ビット値（倍精度浮動小数点数）に対して、FPCはGorillaよりも速いですが、32ビット値に対しては異なる結果が得られる場合があります。可能な `level` の値: 1-28、デフォルト値は12。可能な `float_size` の値: 4, 8、デフォルト値は `sizeof(type)` ですが、タイプがFloatである場合です。それ以外の場合は4です。アルゴリズムの詳細については、[High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf)を参照してください。

#### T64

`T64` — 整数データ型（`Enum`、`Date` および `DateTime` を含む）の値の不要な高ビットを切り落とす圧縮方式です。アルゴリズムのそれぞれのステップで、コーデックは64個の値のブロックを取り、64×64ビットのマトリックスに入れ、転置し、値の不要なビットを切り捨て、残りをシーケンスとして返します。不要なビットは、圧縮に使用されるデータ部分全体で最大値と最小値の間で異ならないビットです。

`DoubleDelta` 及び `Gorilla` コーデックはGorilla TSDBにおいて、その圧縮アルゴリズムのコンポーネントとして使用されます。Gorillaアプローチは、タイムスタンプを持つゆっくりと変化する値のシーケンスシナリオで効果的です。タイムスタンプは `DoubleDelta` コーデックによって効果的に圧縮され、値は `Gorilla` コーデックによって効果的に圧縮されます。例えば、効果的に保存されたテーブルを得るには、以下のような構成でテーブルを作成できます。

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

### 暗号化コーデック

これらのコーデックは実際にはデータを圧縮しないが、代わりにディスク上のデータを暗号化します。これらは、[encryption](../../../operations/server-configuration-parameters/settings.md#server-settings-encryption) 設定によって暗号化キーが指定された場合にのみ利用可能です。なお、暗号化は通常、コーデックパイプラインの最後に意味をなします。なぜなら、暗号化されたデータは通常、意味のある方法で圧縮することができないためです。

暗号化コーデック：

#### AES_128_GCM_SIV

`CODEC('AES-128-GCM-SIV')` — RFC 8452](https://tools.ietf.org/html/rfc8452) GCM-SIV モードで AES-128 を使用してデータを暗号化します。

#### AES-256-GCM-SIV

`CODEC('AES-256-GCM-SIV')` — GCM-SIV モードで AES-256 を使用してデータを暗号化します。

これらのコーデックは固定ノンスを使用し、暗号化は決定的であるため、[ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) などの重複排除エンジンと互換性がありますが欠点があります：同じデータブロックが2回暗号化されると、生成される暗号文はまったく同じになりますので、ディスクを読み取ることができる攻撃者はこの等価性を確認することができます（コンテンツ自体ではないが）。

:::note
"\*MergeTree"ファミリーを含むほとんどのエンジンは、コーデックを適用せずにディスク上にインデックスファイルを作成します。これにより、暗号化されたカラムがインデックス化されると、ディスク上にプレーンテキストが現れます。
:::

:::note
特定の値を暗号化されたカラムで言及する具体的な値を含むSELECTクエリを実行した場合（例：WHERE句内）、その値が[system.query_log](../../../operations/system-tables/query_log.md) に表示されることがあります。ログの記録を無効にすることが考慮されるかもしれません。
:::

**例**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
圧縮を適用する必要がある場合、明示的に指定する必要があります。そうでない場合、データには暗号化のみが適用されます。
:::

**例**

```sql
CREATE TABLE mytable
(
    x String Codec(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

## 一時テーブル

:::note
一時テーブルはレプリケートされていないことにご注意ください。したがって、一時テーブルに挿入されたデータが他のレプリカで使用可能である保証はありません。一時テーブルが便利な主な利用ケースは、単一のセッション中に小さな外部データセットをクエリまたは結合する場合です。
:::

ClickHouseは次の特徴を持つ一時テーブルをサポートしている：

- 一時テーブルはセッションが終了したときに消失します。接続が失われた場合も含まれます。
- エンジンが指定されていない場合、一時テーブルはMemoryテーブルエンジンを使用し、`Replicated`と`KeeperMap`エンジンを除く任意のテーブルエンジンを使用できます。
- 一時テーブルにDBを指定することはできません。データベース外に作成されます。
- 分散クエリ処理では、Memoryエンジンを使用しているクエリで参照される一時テーブルがリモートサーバーに渡されます。

一時テーブルを作成するには、以下の構文を使用します：

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

ほとんどの場合、一時テーブルは手動で作成されるのではなく、クエリの外部データを使用する場合や、分散した `(GLOBAL) IN` のときに作成されます。詳細については該当するセクションを参照してください

エンジンに [ENGINE = Memory](../../../engines/table-engines/special/memory.md) を使用するテーブルを一時テーブルの代わりに使用することもできます。

## REPLACE TABLE

'REPLACE' クエリはテーブルを原子的に更新することを可能にします。

:::note
このクエリは[Atomic](../../../engines/database-engines/atomic.md)データベースエンジンでのみサポートされています。
:::

テーブルからデータを削除する必要がある場合、新しいテーブルを作成して、不要なデータを取得しない `SELECT` ステートメントでそれを埋め、古いテーブルを削除し、新しいものをリネームします：

```sql
CREATE TABLE myNewTable AS myOldTable;
INSERT INTO myNewTable SELECT * FROM myOldTable WHERE CounterID <12345;
DROP TABLE myOldTable;
RENAME TABLE myNewTable TO myOldTable;
```

上記の代わりに以下を使用できます：

```sql
REPLACE TABLE myOldTable ENGINE = MergeTree() ORDER BY CounterID AS SELECT * FROM myOldTable WHERE CounterID <12345;
```

### 構文

``` sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

`CREATE` クエリのすべての構文形式はこのクエリでも機能します。存在しないテーブルに対する `REPLACE` はエラーを引き起こします。

### 例

次のテーブルを考慮します：

```sql
CREATE DATABASE base ENGINE = Atomic;
CREATE OR REPLACE TABLE base.t1 (n UInt64, s String) ENGINE = MergeTree ORDER BY n;
INSERT INTO base.t1 VALUES (1, 'test');
SELECT * FROM base.t1;
```

```text
┌─n─┬─s────┐
│ 1 │ test │
└───┴──────┘
```

`REPLACE` クエリを使用してすべてのデータをクリア：

```sql
CREATE OR REPLACE TABLE base.t1 (n UInt64, s Nullable(String)) ENGINE = MergeTree ORDER BY n;
INSERT INTO base.t1 VALUES (2, null);
SELECT * FROM base.t1;
```

```text
┌─n─┬─s──┐
│ 2 │ \N │
└───┴────┘
```

`REPLACE` クエリを使用してテーブル構造を変更：

```sql
REPLACE TABLE base.t1 (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO base.t1 VALUES (3);
SELECT * FROM base.t1;
```

```text
┌─n─┐
│ 3 │
└───┘
```

## COMMENT 句

テーブルを作成するときにコメントを追加することができます。

**構文**

``` sql
CREATE TABLE db.table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'コメント'
```

**例**

クエリ:

``` sql
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT '一時的なテーブル';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

結果:

```text
┌─name─┬─comment─────────────┐
│ t1   │ 一時的なテーブル    │
└──────┴─────────────────────┘
```

## 関連コンテンツ

- ブログ: [Optimizing ClickHouse with Schemas and Codecs](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- ブログ: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
