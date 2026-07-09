---
description: 'TABLEに関するドキュメント'
keywords: ['圧縮', 'codec', 'スキーマ', 'DDL']
sidebar_label: 'TABLE'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

新しいテーブルを作成します。このクエリは、ユースケースに応じてさまざまな構文形式を取ります。

デフォルトでは、テーブルは現在のサーバーにのみ作成されます。分散 DDL クエリは `ON CLUSTER` 句として実装されており、これについては[別途説明しています](../../../sql-reference/distributed-ddl.md)。

<div id="syntax-forms">
  ## 構文の形式
</div>

<div id="with-explicit-schema">
  ### 明示的なスキーマを指定する場合
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

`db` が設定されている場合は `db` データベースに、設定されていない場合は現在のデータベースに、括弧内で指定した構造と `engine` エンジンを持つ `table_name` という名前のテーブルを作成します。
テーブルの構造は、カラム定義、セカンダリ索引、プロジェクション、制約の一覧です。[主キー](#primary-key) がエンジンでサポートされている場合は、テーブルエンジンのパラメータとして指定されます。

最も単純な場合、カラム定義は `name type` です。例: `RegionID UInt32`。

デフォルト値の式も定義できます (以下を参照) 。

必要に応じて、1 つ以上のキー式を使って主キーを指定できます。

カラムおよびテーブルにコメントを追加できます。

<div id="with-a-schema-similar-to-other-table">
  ### 既存テーブルのスキーマを使用する場合
</div>

ClickHouse では、既存のテーブルのスキーマとデータをコピーできます。

既存のテーブルのスキーマを複製するには、次のようにします。

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

これにより、別のテーブルと同じ構造のテーブルが作成されます。

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### 既存テーブルのスキーマとデータを使用する場合
</div>

既存テーブルのスキーマとデータをレプリケートするには:

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

これにより、既存のテーブルと同じスキーマおよびデータを持つテーブルが作成されます。新しいテーブルが作成されると、`db.table` のすべてのパーティションがそのテーブルにアタッチされます。つまり、`db.table` のデータは作成時に `db2.table_clone` に複製されます。このクエリは、次のクエリと同等です。

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

どちらの機能でも、テーブルに別のエンジンを指定できます。エンジンが指定されていない場合は、元のテーブル (`db.table`) と同じエンジンが使用されます。

<div id="from-a-table-function">
  ### Table Function を使用する場合
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

指定された [table function](/ja/sql-reference/table-functions) と同じ結果を返すテーブルを作成します。作成されたテーブルも、指定した対応するテーブル関数と同じように機能します。

<div id="from-select-query">
  ### SELECTクエリから
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

`SELECT` クエリの結果と同様の構造を持つ `engine` エンジンのテーブルを作成し、`SELECT` のデータを挿入します。カラムの定義を明示的に指定することもできます。

テーブルがすでに存在し、`IF NOT EXISTS` が指定されている場合、このクエリは何も実行しません。

このクエリでは、`ENGINE` 句の後にほかの句を指定することもできます。テーブルの作成方法の詳細については、[テーブルエンジン](/ja/engines/table-engines) の説明を参照してください。

**例**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## `NULL` または `NOT NULL` 修飾子
</div>

カラム定義でデータ型の後に付ける `NULL` および `NOT NULL` 修飾子は、その型を [Nullable](/ja/sql-reference/data-types/nullable) にするかどうかを指定します。

型が `Nullable` でない場合、`NULL` が指定されると `Nullable` として扱われますが、`NOT NULL` が指定された場合はそうなりません。たとえば、`INT NULL` は `Nullable(INT)` と同じです。型が `Nullable` の場合に `NULL` または `NOT NULL` 修飾子を指定すると、例外がスローされます。

関連項目: [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable) 設定。

<div id="default_values">
  ## デフォルト値
</div>

カラム定義では、`DEFAULT expr`、`MATERIALIZED expr`、または `ALIAS expr` の形式でデフォルト値の式を指定できます。例: `URLDomain String DEFAULT domain(URL)`。

式 `expr` は省略可能です。省略した場合は、カラム型を明示的に指定する必要があります。このとき、デフォルト値は数値カラムでは `0`、String 型のカラムでは `''` (空文字列) 、Array 型のカラムでは `[]` (空の配列) 、Date 型のカラムでは `1970-01-01`、Nullable 型のカラムでは `NULL` になります。

デフォルト値カラムのカラム型は省略することもでき、その場合は `expr` の型から推論されます。たとえば、`EventDate DEFAULT toDate(EventTime)` というカラムの型は Date になります。

データ型とデフォルト値の式の両方が指定されている場合は、式を指定された型に変換する暗黙的な型キャスト関数が挿入されます。例: `Hits UInt32 DEFAULT 0` は内部的に `Hits UInt32 DEFAULT toUInt32(0)` として表現されます。

デフォルト値の式 `expr` は、任意のテーブルカラムや定数を参照できます。ClickHouse は、テーブル構造の変更によって式の計算にループが生じないことを確認します。INSERT については、式を解決可能であること、つまり式の計算元となるすべてのカラムが渡されていることを確認します。

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

通常のデフォルト値です。このようなカラムの値が INSERT クエリで指定されていない場合は、`expr` から算出されます。

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

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

マテリアライズド式です。このようなカラムの値は、行の挿入時に指定されたマテリアライズド式に従って自動的に計算されます。`INSERT` 時に値を明示的に指定することはできません。

また、この型のデフォルト値カラムは `SELECT *` の結果に含まれません。これは、`SELECT *` の結果を常に `INSERT` を使ってテーブルにそのまま挿入し直せるという不変条件を保つためです。この動作は、設定 `asterisk_include_materialized_columns` で無効にできます。

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

INSERT INTO test VALUES (1);

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

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

一時的なカラムです。この型のカラムはテーブルに保存されず、これらに対して `SELECT` することもできません。EPHEMERAL カラムの唯一の用途は、これらを使って他のカラムのデフォルト値の式を構築することです。

明示的にカラムを指定しない insert では、この型のカラムはスキップされます。これは、`SELECT *` の結果を常に `INSERT` を使ってテーブルに戻せるという不変条件を保つためです。

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

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

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

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

計算カラム (同義語) 。この型のカラムはテーブルに保存されず、これらに値を INSERT することはできません。

SELECT クエリでこの型のカラムを明示的に参照すると、その値はクエリ実行時に `expr` から計算されます。デフォルトでは、`SELECT *` に ALIAS カラムは含まれません。この動作は、設定 `asterisk_include_alias_columns` で無効にできます。

ALTER クエリを使用して新しいカラムを追加しても、それらのカラムに対して古いデータが書き込まれることはありません。代わりに、新しいカラムの値を持たない古いデータを読み取る際には、デフォルトで式がその場で計算されます。ただし、その式の実行にクエリで指定されていない別のカラムが必要な場合は、必要なデータブロックに対してのみ、それらのカラムも追加で読み取られます。

テーブルに新しいカラムを追加した後でそのデフォルト式を変更すると、古いデータに使用される値も変わります (ディスクに値が保存されていないデータについて) 。なお、バックグラウンドマージの実行時には、マージ対象のいずれかのパーツに存在しないカラムのデータは、マージ後のパーツに書き込まれます。

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

<div id="primary-key">
  ## 主キー
</div>

テーブルの作成時に[主キー](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)を定義できます。主キーは次の 2 つの方法で指定できます。

* カラム一覧の中で

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* カラム一覧の外

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
1つのクエリの中で、2つの方法を併用することはできません。
:::

<div id="constraints">
  ## 制約
</div>

カラムの説明に加え、制約を定義することもできます。

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` には任意のブール式を指定できます。テーブルに制約が定義されている場合、`INSERT` クエリでは各行に対してそれぞれの制約がチェックされます。いずれかの制約が満たされない場合、サーバーは制約名とチェック式を含む例外を返します。

大量の制約を追加すると、大規模な `INSERT` クエリのパフォーマンスに悪影響を与える可能性があります。

すべてのテーブルに存在する既存の制約は、[`system.constraints`](/ja/operations/system-tables/constraints) テーブルで確認できます。

<div id="assume">
  ### ASSUME
</div>

`ASSUME` 句は、true とみなされるテーブル上の `CONSTRAINT` を定義するために使用します。この制約は、その後オプティマイザによって SQL クエリのパフォーマンス向上に利用されます。

次の例では、`users_a` テーブルの作成時に `ASSUME CONSTRAINT` を使用しています。

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

ここでは、`ASSUME CONSTRAINT` を使って、`length(name)` 関数が常に `name_len` カラムの値と等しいことを前提として指定しています。これは、クエリ内で `length(name)` が呼び出されるたびに、ClickHouse がそれを `name_len` に置き換えられることを意味します。`length()` 関数を呼び出さずに済むため、その分高速化が期待できます。

そのため、クエリ `SELECT name FROM users_a WHERE length(name) < 5;` を実行する際、ClickHouse は `ASSUME CONSTRAINT` に基づいてこれを `SELECT name FROM users_a WHERE name_len < 5` に最適化できます。これにより、各行ごとに `name` の長さを計算する必要がなくなり、クエリの実行が高速になる可能性があります。

`ASSUME CONSTRAINT` は**制約を強制するものではありません**。あくまで、その制約が成り立つことをオプティマイザに伝えるだけです。実際には制約が成り立っていない場合、クエリ結果が不正確になるおそれがあります。したがって、`ASSUME CONSTRAINT` を使用するのは、その制約が正しいと確信できる場合に限るべきです。

<div id="ttl-expression">
  ## 有効期限 (TTL) 式
</div>

値の保存期間を定義します。指定できるのは MergeTree ファミリーのテーブルのみです。詳細については、[カラムとテーブルの TTL](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)を参照してください。

<div id="column_compression_codec">
  ## カラムの圧縮コーデック
</div>

デフォルトでは、ClickHouse はセルフマネージド版では `lz4` 圧縮を、ClickHouse Cloud では `zstd` を適用します。

`MergeTree` エンジンファミリーでは、サーバー設定の [compression](/ja/operations/server-configuration-parameters/settings#compression) セクションで、デフォルトの圧縮方式を変更できます。

また、`CREATE TABLE` クエリで各カラムの圧縮方式を個別に定義することもできます。

```sql
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

`Default` コーデック を指定すると、実行時の各種設定 (およびデータの特性) に応じたデフォルトの圧縮を参照できます。
Example: `value UInt64 CODEC(Default)` — コーデック を指定しない場合と同じです。

また、カラムから現在の CODEC を削除して、config.xml のデフォルト圧縮を使用することもできます:

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

コーデックはパイプラインで組み合わせることができます。たとえば、`CODEC(Delta, Default)` のように指定します。

:::tip
ClickHouse データベースのファイルは、`lz4` のような外部ユーティリティでは解凍できません。代わりに、専用の [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) ユーティリティを使用してください。
:::

圧縮は、次のテーブルエンジンでサポートされています。

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) ファミリー。カラム圧縮コーデックと、[compression](/ja/operations/server-configuration-parameters/settings#compression) 設定によるデフォルトの圧縮方式の選択をサポートします。
* [Log](../../../engines/table-engines/log-family/index.md) ファミリー。デフォルトで `lz4` 圧縮方式を使用し、カラム圧縮コーデックをサポートします。
* [Set](../../../engines/table-engines/special/set.md)。デフォルトの圧縮のみをサポートします。
* [Join](../../../engines/table-engines/special/join.md)。デフォルトの圧縮のみをサポートします。

ClickHouse は汎用コーデックと特殊コーデックをサポートしています。

<div id="general-purpose-codecs">
  ### 汎用コーデック
</div>

<div id="none">
  #### NONE
</div>

`NONE` — 圧縮なし。

<div id="lz4">
  #### LZ4
</div>

`LZ4` — 既定で使用される、可逆の[データ圧縮アルゴリズム](https://github.com/lz4/lz4)です。LZ4の高速圧縮を適用します。

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — レベルを設定可能な LZ4 HC (高圧縮) アルゴリズムです。デフォルトのレベルは 9 です。`level <= 0` を設定すると、デフォルトのレベルが適用されます。指定可能なレベルは [1, 12] です。推奨レベルの範囲は [4, 9] です。

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — `level`を設定可能な[ZSTD圧縮アルゴリズム](https://en.wikipedia.org/wiki/Zstandard)です。指定可能なレベル: [1, 22]。デフォルトのレベル: 1。

高い圧縮レベルは、1回圧縮して何度も展開するような非対称なシナリオで有効です。レベルが高いほど圧縮率は向上しますが、CPU使用量も増加します。

<div id="zstd_qat">
  #### 廃止された: ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### 廃止された: DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### 特殊用途向けコーデック
</div>

これらのコーデックは、データの特性を利用して、圧縮をより効果的に行えるよう設計されています。これらのコーデックの一部は、それ自体ではデータを圧縮せず、代わりにデータを前処理することで、汎用コーデックによる第2段階の圧縮で、より高い圧縮率を実現できるようにします。

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — 最初の値をそのまま保持し、それ以降の生の値を隣接する2つの値の差に置き換える圧縮方式です。`delta_bytes` は生の値の最大サイズで、デフォルト値は `sizeof(type)` です。`delta_bytes` を引数として指定することは非推奨であり、今後のリリースでサポートは削除される予定です。Delta はデータ前処理用の コーデック であるため、単独では使用できません。

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — デルタの差分を計算し、compact パーツなバイナリ形式で書き込みます。`bytes_size` は、[Delta](#delta) コーデックにおける `delta_bytes` と同様の意味を持ちます。引数として `bytes_size` を指定することは非推奨であり、今後のリリースでサポートが削除される予定です。時系列データのように、ストライドが一定の単調な数列に対して最適な圧縮率が得られます。任意の数値型で使用できます。Gorilla TSDB で使われているアルゴリズムを実装し、64 ビット型をサポートするよう拡張しています。32 ビットのデルタでは 1 ビット余分に使用します。つまり、4 ビットのプレフィックスではなく 5 ビットのプレフィックスです。詳細は、[Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) の「Compressing Time Stamps」を参照してください。DoubleDelta はデータ準備用のコーデックであり、単独では使用できません。

<div id="gcd">
  #### GCD
</div>

`GCD()` - - カラム内の値の最大公約数 (GCD) を計算し、各値をその GCD で割ります。整数、Decimal、日付/時刻カラムで使用できます。このコーデックは、値が GCD の倍数刻みで変化 (増減) するカラム、たとえば 24, 28, 16, 24, 8, 24 (GCD = 4) に適しています。GCD はデータ準備コーデックであり、単独では使用できません。

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — 現在の浮動小数点値と直前の浮動小数点値の XOR を計算し、それをコンパクトなバイナリ形式で書き込みます。連続する値の差が小さいほど、つまり時系列の値の変化が緩やかであるほど、圧縮率は高くなります。Gorilla TSDB で使われているアルゴリズムを実装しており、64 ビット型をサポートするよう拡張されています。`bytes_size` に指定できる値は 1、2、4、8 です。デフォルト値は、`sizeof(type)` が 1、2、4、8 のいずれかであれば `sizeof(type)` になり、それ以外の場合は 1 になります。詳細については、[Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078) の 4.1 節を参照してください。

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — 浮動小数点データ向けの適応型可逆圧縮です。`Float32` と `Float64` をサポートします。詳細は [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334) を参照してください。

このコーデックは、省略可能な variant 引数を受け付けます。

* `ALP()` または `ALP(AUTO)` (デフォルト) — 推定された圧縮サイズに基づいて STD を使用し、必要に応じて RD にフォールバックします。
* `ALP(STD)` — 標準の ALP variant です。各値を 10 の累乗による厳密なスケーリング整数として表現し、その後、得られた整数を Frame-of-Reference とビットパッキングで圧縮します。表現できない値は、生の例外として保存されます。10 進数に由来する数値 (たとえば測定値や価格) に最適です。
* `ALP(RD)` — Real Doubles variant です。各値のビットパターンを再解釈し、上位部分 (符号 + 指数 + 仮数の上位ビット) と下位部分に分割します。上位部分は Dictionary エンコードされ (最大 8 エントリ) 、下位部分はビットパッキングされます。多くの値で同じ上位ビットが共有される場合に最適です。

:::note
このコーデックは Experimental であり、使用するには `SET allow_experimental_codecs = 1` が必要です。
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - 2 つの予測器のうち精度の高い方を使って、数列内の次の浮動小数点値を繰り返し予測し、実際の値と予測値に XOR を適用したうえで、その結果を先頭のゼロを利用して圧縮します。Gorilla と同様に、変化が緩やかな一連の浮動小数点値を保存する場合に効率的です。64 ビット値 (double) では FPC は Gorilla より高速ですが、32 ビット値では効果は状況によって異なります。指定可能な `level` の値は 1-28 で、デフォルト値は 12 です。指定可能な `float_size` の値は 4、8 で、型が Float の場合のデフォルト値は `sizeof(type)` です。それ以外の場合は 4 です。アルゴリズムの詳細については、[High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf) を参照してください。

<div id="t64">
  #### T64
</div>

`T64` — 整数データ型 (`Enum`、`Date`、`DateTime` を含む) の値から、使われていない上位ビットを切り詰める圧縮方式です。アルゴリズムの各ステップで、この コーデック は 64 個の値からなるブロックを取り出し、それらを 64x64 のビット行列に配置して転置し、値の未使用ビットを切り詰めたうえで、残りを数列として返します。未使用ビットとは、この圧縮が適用される data part 全体において、最大値と最小値の間で差がないビットのことです。

`DoubleDelta` コーデック と `Gorilla` コーデック は、Gorilla TSDB の圧縮アルゴリズムを構成する要素として使用されます。Gorilla の方式は、値とそれに対応するタイムスタンプが緩やかに変化する数列がある場合に効果的です。タイムスタンプは `DoubleDelta` コーデック によって効率よく圧縮され、値は `Gorilla` コーデック によって効率よく圧縮されます。たとえば、効率よく格納できるテーブルを得るには、次の構成で作成できます。

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### 暗号化コーデック
</div>

これらのコーデックは実際にはデータを圧縮せず、代わりにディスク上のデータを暗号化します。これらを利用できるのは、[encryption](/ja/operations/server-configuration-parameters/settings#encryption) 設定で暗号化キーが指定されている場合のみです。通常、暗号化されたデータは意味のある形では圧縮できないため、暗号化はコーデックパイプラインの末尾でのみ有効である点に注意してください。

暗号化コーデック:

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — データを、[RFC 8452](https://tools.ietf.org/html/rfc8452) で定義されている AES-128 の GCM-SIV モードで暗号化します。

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — AES-256 を GCM-SIV モードで使用してデータを暗号化します。

これらのコーデックは固定 nonce を使用するため、暗号化は決定論的です。そのため、[ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) のような重複排除を行うエンジンと互換性がありますが、弱点もあります。同じデータブロックを 2 回暗号化すると、生成される暗号文はまったく同じになるため、ディスクを読み取れる攻撃者はその一致を見て判別できます (ただし、わかるのは一致していることだけで、内容そのものは取得できません) 。

:::note
&quot;*MergeTree&quot; ファミリーを含むほとんどのエンジンでは、コーデックを適用せずにディスク上に索引ファイルが作成されます。つまり、暗号化されたカラムに索引がある場合、平文がディスク上に現れます。
:::

:::note
暗号化されたカラム内の特定の値を指定する SELECT クエリを実行すると (たとえば WHERE 句で) 、その値が [system.query&#95;log](../../../operations/system-tables/query_log.md) に記録される可能性があります。ログを無効にすることを検討してください。
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
圧縮が必要な場合は、明示的に指定する必要があります。指定しない場合、データには暗号化のみが適用されます。
:::

**例**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## 一時テーブル
</div>

:::note
一時テーブルはレプリケートされない点に注意してください。そのため、一時テーブルに挿入されたデータが他のレプリカでも利用できるとは限りません。一時テーブルが主に役立つのは、単一のセッション中に小規模な外部データセットをクエリしたり結合したりする場合です。
:::

ClickHouse は、次のような特徴を持つ一時テーブルをサポートしています。

* 一時テーブルは、接続が失われた場合を含め、セッションが終了すると消えます。
* 一時テーブルでは、エンジンが指定されていない場合は Memory テーブルエンジンが使用され、Replicated および `KeeperMap` エンジンを除く任意のテーブルエンジンを使用できます。
* 一時テーブルには DB を指定できません。データベースの外に作成されます。
* 分散 DDL クエリでクラスター内のすべてのサーバーに一時テーブルを作成することはできません (`ON CLUSTER` を使用) 。このテーブルは現在のセッションにのみ存在します。
* 一時テーブルが別のテーブルと同じ名前で、クエリ内で DB を指定せずにテーブル名だけを指定した場合は、一時テーブルが使用されます。
* 分散クエリ処理では、クエリで使用される Memory エンジンの一時テーブルがリモートサーバーに渡されます。

一時テーブルを作成するには、次の構文を使用します。

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

ほとんどの場合、一時テーブルを手動で作成することはありませんが、クエリで外部データを使用する場合や、分散 `(GLOBAL) IN` を使用する場合には作成されます。詳細については、該当するセクションを参照してください

一時テーブルの代わりに [ENGINE = Memory](../../../engines/table-engines/special/memory.md) を使用することもできます。

<div id="replace-table">
  ## REPLACE TABLE
</div>

`REPLACE` ステートメントを使用すると、テーブルを[アトミックに](/ja/concepts/glossary#atomicity)更新できます。

:::note
このステートメントは、[`Atomic`](../../../engines/database-engines/atomic.md) および [`Replicated`](../../../engines/database-engines/replicated.md) データベースエンジンでサポートされています。
これらはそれぞれ、ClickHouse と ClickHouse Cloud のデフォルトのデータベースエンジンです。
:::

通常、テーブルから一部のデータを削除する必要がある場合は、
新しいテーブルを作成し、不要なデータを取得しない `SELECT` ステートメントを使ってそのテーブルにデータを格納し、
その後、古いテーブルを削除して新しいテーブルをリネームできます。
この方法は、以下の例で示しています。

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

上記の方法の代わりに、 (デフォルトのデータベースエンジンを使用している場合は) `REPLACE` を使って同じ結果を得ることもできます。

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### 構文
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
`CREATE` ステートメントのすべての構文形式は、このステートメントでも有効です。存在しないテーブルに対して `REPLACE` を実行すると、エラーが発生します。
:::

<div id="examples">
  ### 例:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="ローカル" default>
    次のテーブルを見てみましょう:

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    `REPLACE` ステートメントを使用すると、すべてのデータを削除できます:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    また、`REPLACE` ステートメントを使用してテーブル構造を変更することもできます:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    ClickHouse Cloud 上の次のテーブルを見てみましょう:

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    `REPLACE` ステートメントを使用すると、すべてのデータを削除できます:

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    また、`REPLACE` ステートメントを使用してテーブル構造を変更することもできます:

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## COMMENT 句
</div>

テーブルの作成時にコメントを追加できます。

**構文**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
`COMMENT` 句は、`PARTITION BY`、`ORDER BY`、ストレージ固有の `SETTINGS` など、ストレージ固有の句の**後**に指定する必要があります。

`COMMENT` 句の後で解釈されるのは、クエリ固有の `SETTINGS` (`max_threads` など) のみであり、ストレージ関連の設定は解釈されません。

つまり、正しい句の順序は次のとおりです。

* `ENGINE`
* ストレージ句
* `COMMENT`
* クエリ設定 (存在する場合)
  :::

**例**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## 関連記事
</div>

* ブログ: [スキーマとコーデックで ClickHouse を最適化する](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* ブログ: [ClickHouse で時系列データを扱う](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)