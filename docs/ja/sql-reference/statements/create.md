---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: CREATE
---

# クエリの作成 {#create-queries}

## CREATE DATABASE {#query-language-create-database}

データベースの作成

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### 句 {#clauses}

-   `IF NOT EXISTS`
    もし `db_name` データベースが既に存在する場合、ClickHouseは新しいデータベースを作成せず、:

    -   句が指定されている場合は例外をスローしません。
    -   句が指定されていない場合、例外をスローします。

-   `ON CLUSTER`
    クリックハウスは `db_name` データベースの全てのサーバーの指定されたクラスター

-   `ENGINE`

    -   [MySQL](../../engines/database-engines/mysql.md)
        リモートMySQLサーバーからデータを取得できます。
        既定では、ClickHouseは独自のものを使用します [データベース](../../engines/database-engines/index.md).

## CREATE TABLE {#create-table-query}

その `CREATE TABLE` クエリには複数の形式があります。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

テーブルを作成します ‘name’ で ‘db’ データベー ‘db’ が設定されていない。 ‘engine’ エンジン
テーブルの構造は、列の説明のリストです。 た場合の指数については、エンジンとして表示していパラメータテーブルのエンジンです。

列の説明は次のとおりです `name type` 最も単純なケースでは。 例: `RegionID UInt32`.
デフォルト値に対して式を定義することもできます(下記参照)。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

別のテーブルと同じ構造を持つテーブルを作成します。 テーブルに別のエンジンを指定できます。 エンジンが指定されていない場合、同じエンジンが使用されます。 `db2.name2` テーブル。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

テーブルを作成しますの構造やデータによって返される [テーブル関数](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

の結果のような構造体を持つテーブルを作成します。 `SELECT` クエリは、 ‘engine’ エンジン充填でのデータを選択します。

すべての場合において、 `IF NOT EXISTS` テーブルが既に存在する場合、クエリはエラーを返しません。 この場合、クエリは何もしません。

の後に他の句がある場合もあります。 `ENGINE` クエリ内の句。 テーブルの作成方法に関する詳細なドキュメントを参照してください [表エンジン](../../engines/table-engines/index.md#table_engines).

### デフォルト値 {#create-default-values}

列の説明では、次のいずれかの方法で既定値の式を指定できます:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
例: `URLDomain String DEFAULT domain(URL)`.

既定値の式が定義されていない場合、既定値は数値の場合はゼロ、文字列の場合は空の文字列、配列の場合は空の配列、および `1970-01-01` 日付または zero unix timestamp 時間と日付のために。 Nullはサポートされていません。

既定の式が定義されている場合、列の型は省略可能です。 明示的に定義された型がない場合は、既定の式の型が使用されます。 例: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ タイプは ‘EventDate’ 列。

データ型と既定の式が明示的に定義されている場合、この式は型キャスト関数を使用して指定された型にキャストされます。 例: `Hits UInt32 DEFAULT 0` と同じことを意味します `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don't contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

標準のデフォルト値。 INSERTクエリで対応する列が指定されていない場合は、対応する式を計算して入力されます。

`MATERIALIZED expr`

マテリアライズ式。 このような列は常に計算されるため、INSERTには指定できません。
列のリストがない挿入の場合、これらの列は考慮されません。
また、SELECTクエリでアスタリスクを使用する場合、この列は置換されません。 これは、ダンプが取得した不変量を保持するためです `SELECT *` 列のリストを指定せずにINSERTを使用してテーブルに挿入することができます。

`ALIAS expr`

シノニム このような列は、テーブルにまったく格納されません。
また、SELECTクエリでアスタリスクを使用する場合は、その値は置換されません。
クエリの解析中にエイリアスが展開される場合は、Selectで使用できます。

ALTERクエリを使用して新しい列を追加する場合、これらの列の古いデータは書き込まれません。 代わりに、新しい列の値を持たない古いデータを読み取るときは、デフォルトで式がその場で計算されます。 ただし、式を実行するには、クエリで指定されていない異なる列が必要な場合は、これらの列はさらに読み取られますが、必要なデータブロックに対し

テーブルに新しい列を追加し、後でその既定の式を変更すると、古いデータに使用される値が変更されます（値がディスクに格納されていないデータの場合）。 ご注意実行する場合背景が合併のデータ列を欠損の合流部に書かれて合併します。

入れ子になったデータ構造の要素に既定値を設定することはできません。

### 制約 {#constraints}

列記述制約と一緒に定義することができます:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` 任意のブール式で可能です。 場合に制約の定義のテーブルのそれぞれチェック毎に行 `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

追加大量の制約になる可能性の性能を大 `INSERT` クエリ。

### TTL式 {#ttl-expression}

値の保存時間を定義します。 MergeTree-familyテーブルに対してのみ指定できます。 詳細な説明については、 [列および表のTTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### 列圧縮コーデック {#codecs}

デフォルトでは、ClickHouseは `lz4` 圧縮方法。 のために `MergeTree`-エンジンファミリでデフォルトの圧縮方法を変更できます。 [圧縮](../../operations/server-configuration-parameters/settings.md#server-settings-compression) サーバー構成のセクション。 また、圧縮方法を定義することもできます。 `CREATE TABLE` クエリ。

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

コーデックが指定されている場合、既定のコーデックは適用されません。 コーデックはパイプラインで結合できます。, `CODEC(Delta, ZSTD)`. の選定と大型ブリッジダイオードコーデックの組み合わせますプロジェクト、ベンチマークと同様に記載のAltinity [ClickHouseの効率を改善する新しい符号化](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) 記事だ

!!! warning "警告"
    できない解凍ClickHouseデータベースファイルを外部の事のように `lz4`. 代わりに、特別な [clickhouse-コンプレッサー](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) ユーティリティ

圧縮は、次の表エンジンでサポートされます:

-   [メルゲツリー](../../engines/table-engines/mergetree-family/mergetree.md) 家族だ 支柱の圧縮コーデックとの選択のデフォルトの圧縮メソッドによる [圧縮](../../operations/server-configuration-parameters/settings.md#server-settings-compression) 設定。
-   [ログ](../../engines/table-engines/log-family/index.md) 家族だ を使用して `lz4` 圧縮メソッドはデフォルト対応カラムの圧縮コーデック.
-   [セット](../../engines/table-engines/special/set.md). 既定の圧縮のみをサポートしました。
-   [参加](../../engines/table-engines/special/join.md). 既定の圧縮のみをサポートしました。

ClickHouse支援共通の目的コーデックや専門のコーデック.

#### 特殊なコーデック {#create-query-specialized-codecs}

これらのコーデックしていただくための圧縮により効果的な利用の特徴データです。 これらのコーデックの一部は、データ自体を圧縮しません。 代わりに、共通の目的のコーデックのためにデータを準備し、この準備がない場合よりも圧縮します。

特殊なコーデック:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` デルタ値を格納するために使用されます。 `delta_bytes` 生の値の最大サイズです。 可能 `delta_bytes` 値:1,2,4,8。 のデフォルト値 `delta_bytes` は `sizeof(type)` 1、2、4、または8に等しい場合。 他のすべてのケースでは、それは1です。
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla:高速でスケーラブルなメモリ内の時系列データベース](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorilla:高速でスケーラブルなメモリ内の時系列データベース](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` と `DateTime`). そのアルゴリズムの各ステップで、codecは64の値のブロックを取り、それらを64x64ビット行列に入れ、転置し、未使用の値のビットを切り取り、残りをシー 未使用ビットは、圧縮が使用されるデータ部分全体の最大値と最小値の間で異ならないビットです。

`DoubleDelta` と `Gorilla` Gorilla TSDBでは圧縮アルゴリズムの構成要素としてコーデックが使用されている。 Gorillaのアプローチは、タイムスタンプにゆっくりと変化する一連の値がある場合のシナリオで効果的です。 タイムスタンプは `DoubleDelta` によって効果的に圧縮されます `Gorilla` コーデック。 たとえば、効果的に格納されたテーブルを取得するには、次の構成で作成できます:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### 汎用コーデック {#create-query-general-purpose-codecs}

コーデック:

-   `NONE` — No compression.
-   `LZ4` — Lossless [データ圧縮](https://github.com/lz4/lz4) 既定で使用されます。 LZ4高速圧縮を適用します。
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` 既定のレベルを適用します。 可能なレベル：\[1、12\]。 推奨レベル範囲：\[4、9\]。
-   `ZSTD[(level)]` — [ZSTD圧縮アルゴリズム](https://en.wikipedia.org/wiki/Zstandard) 構成可能を使って `level`. 可能なレベル：\[1、22\]。 デフォルト値:1。

高い圧縮レベルは、一度の圧縮、繰り返しの解凍などの非対称シナリオに役立ちます。 高いレベルは、より良い圧縮と高いCPU使用率を意味します。

## 一時テーブル {#temporary-tables}

ClickHouseは、次の特性を持つ一時テーブルをサポートします:

-   一時テーブルは、接続が失われた場合を含め、セッションが終了すると消滅します。
-   一時テーブルはメモリエンジンのみを使用します。
-   一時テーブルにDBを指定することはできません。 データベースの外部に作成されます。
-   すべてのクラスタサーバーで分散DDLクエリを使用して一時テーブルを作成することはできません `ON CLUSTER`):このテーブルは現在のセッションにのみ存在します。
-   一時テーブルの名前が別のテーブルと同じで、クエリがDBを指定せずにテーブル名を指定した場合、一時テーブルが使用されます。
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

ほとんどの場合、一時テーブルを手動で作成され、外部データを利用するためのクエリに対して、または配布 `(GLOBAL) IN`. 詳細については、該当する項を参照してください

テーブルを使用することは可能です [エンジン=メモリ](../../engines/table-engines/special/memory.md) 一時テーブルの代わりに。

## 分散DDLクエリ(ON CLUSTER句) {#distributed-ddl-queries-on-cluster-clause}

その `CREATE`, `DROP`, `ALTER`,and `RENAME` クエリの支援の分散実行クラスター
たとえば、次のクエリは、 `all_hits` `Distributed` 各ホストのテーブル `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

これらのクエリを正しく実行するには、各ホストが同じクラスタ定義を持っている必要があります（設定の同期を簡単にするために、ZooKeeperからの置換 また、ZooKeeperサーバーに接続する必要があります。
一部のホストが現在利用できない場合でも、ローカルバージョンのクエリは最終的にクラスタ内の各ホストに実装されます。 単一のホスト内でクエリを実行する順序は保証されます。

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

ビューを作成します。 通常と実体化：ビューの二つのタイプがあります。

通常のビューはデータを格納せず、別のテーブルからの読み取りを実行するだけです。 つまり、通常のビューは保存されたクエリに過ぎません。 ビューから読み取るとき、この保存されたクエリはFROM句のサブクエリとして使用されます。

例として、ビューを作成したとします:

``` sql
CREATE VIEW view AS SELECT ...
```

そして、クエリを書いた:

``` sql
SELECT a, b, c FROM view
```

このクエリは、サブクエリの使用と完全に同等です:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

マテリアライズドビュ

マテリアライズドビューを作成するとき `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

マテリアライズドビューを作成するとき `TO [db].[table]`、使用してはいけません `POPULATE`.

SELECTで指定されたテーブルにデータを挿入すると、挿入されたデータの一部がこのSELECTクエリによって変換され、結果がビューに挿入されます。

POPULATEを指定すると、既存のテーブルデータが作成時にビューに挿入されます。 `CREATE TABLE ... AS SELECT ...` . そうしないと、クエリーを含み、データを挿入し、表の作成後、作成した。 ビューの作成中にテーブルに挿入されたデータは挿入されないため、POPULATEを使用することはお勧めしません。

A `SELECT` クエリを含むことができ `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` データは挿入中に集計されますが、挿入されたデータの単一パケット内でのみ集計されます。 データはそれ以上集計されません。 例外は、次のように独立してデータ集計を実行するエンジンを使用する場合です `SummingMergeTree`.

の実行 `ALTER` マテリアライズドビューのクエリは完全に開発されていないため、不便な場合があります。 マテリアライズドビューで構成を使用する場合 `TO [db.]name`、できます `DETACH` ビュー、実行 `ALTER` ターゲットテーブルの場合、次に `ATTACH` 以前に切り離された (`DETACH`）ビュー。

ビューの外観は通常のテーブルと同じです。 たとえば、それらは次の結果にリストされます `SHOW TABLES` クエリ。

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

作成 [外部辞書](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) 与えられたと [構造](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [ソース](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [レイアウト](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) と [生涯](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

外部辞書構造は属性で構成されます。 辞書属性は、表の列と同様に指定されます。 必要な属性プロパティはその型だけで、他のすべてのプロパティには既定値があります。

辞書によっては [レイアウト](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) 一つ以上の属性を辞書キーとして指定できます。

詳細については、 [外部辞書](../dictionaries/external-dictionaries/external-dicts.md) セクション

## CREATE USER {#create-user-statement}

を作成します。 [ユーザー](../../operations/access-rights.md#user-account-management).

### 構文 {#create-user-syntax}

``` sql
CREATE USER [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IDENTIFIED [WITH {NO_PASSWORD|PLAINTEXT_PASSWORD|SHA256_PASSWORD|SHA256_HASH|DOUBLE_SHA1_PASSWORD|DOUBLE_SHA1_HASH}] BY {'password'|'hash'}]
    [HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

#### 識別情報 {#identification}

ユーザー識別には複数の方法があります:

-   `IDENTIFIED WITH no_password`
-   `IDENTIFIED WITH plaintext_password BY 'qwerty'`
-   `IDENTIFIED WITH sha256_password BY 'qwerty'` または `IDENTIFIED BY 'password'`
-   `IDENTIFIED WITH sha256_hash BY 'hash'`
-   `IDENTIFIED WITH double_sha1_password BY 'qwerty'`
-   `IDENTIFIED WITH double_sha1_hash BY 'hash'`

#### ユーザホスト {#user-host}

ユーザーホストは、ClickHouseサーバーへの接続を確立できるホストです。 ホストを指定することができます。 `HOST` 次の方法によるクエリのセクション:

-   `HOST IP 'ip_address_or_subnetwork'` — User can connect to ClickHouse server only from the specified IP address or a [サブネットワーク](https://en.wikipedia.org/wiki/Subnetwork). 例: `HOST IP '192.168.0.0/16'`, `HOST IP '2001:DB8::/32'`. の使用、生産を指定するだけでいいのです。 `HOST IP` 要素（IPアドレスとそのマスク）を使用しているため `host` と `host_regexp` が原因別の待ち時間をゼロにすることに
-   `HOST ANY` — User can connect from any location. This is default option.
-   `HOST LOCAL` — User can connect only locally.
-   `HOST NAME 'fqdn'` — User host can be specified as FQDN. For example, `HOST NAME 'mysite.com'`.
-   `HOST NAME REGEXP 'regexp'` — You can use [pcre](http://www.pcre.org/) ユーザーホストを指定するときの正規表現。 例えば, `HOST NAME REGEXP '.*\.mysite\.com'`.
-   `HOST LIKE 'template'` — Allows you use the [LIKE](../functions/string-search-functions.md#function-like) ユーザーホストをフィルタする演算子。 例えば, `HOST LIKE '%'` に等しい。 `HOST ANY`, `HOST LIKE '%.mysite.com'` すべてのホストをフィルタする。 `mysite.com` ドメイン。

Hostを指定する別の方法は次のとおりです `@` ユーザー名の構文。 例:

-   `CREATE USER mira@'127.0.0.1'` — Equivalent to the `HOST IP` 構文。
-   `CREATE USER mira@'localhost'` — Equivalent to the `HOST LOCAL` 構文。
-   `CREATE USER mira@'192.168.%.%'` — Equivalent to the `HOST LIKE` 構文。

!!! info "警告"
    クリックハウス `user_name@'address'` 全体としてのユーザー名として。 このように、技術的に作成できます複数のユーザー `user_name` そして後の異なった構造 `@`. 私たちはそうすることをお勧めしません。

### 例 {#create-user-examples}

ユーザーアカウントの作成 `mira` パスワードで保護 `qwerty`:

``` sql
CREATE USER mira HOST IP '127.0.0.1' IDENTIFIED WITH sha256_password BY 'qwerty'
```

`mira` 開始すべきお客様のアプリをホストにClickHouseサーバー運行しています。

ユーザーアカウントの作成 `john` ロールを割り当て、このロールをデフォルトにする:

``` sql
CREATE USER john DEFAULT ROLE role1, role2
```

ユーザーアカウントの作成 `john` 彼の将来の役割をすべてデフォルトにする:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

いくつかの役割が割り当てられるとき `john` 将来的には自動的にデフォルトになります。

ユーザーアカウントの作成 `john` く全ての彼の今後の役割はデフォルトを除く `role1` と `role2`:

``` sql
ALTER USER john DEFAULT ROLE ALL EXCEPT role1, role2
```

## CREATE ROLE {#create-role-statement}

を作成します。 [役割](../../operations/access-rights.md#role-management).

### 構文 {#create-role-syntax}

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### 説明 {#create-role-description}

役割は、 [特権](grant.md#grant-privileges). ロールで付与されたユーザーは、このロールのすべての権限を取得します。

ユーザーの割り当てることができ複数の役割です。 ユーザーに適用できるのを助の役割の任意の組み合わせによる [SET ROLE](misc.md#set-role-statement) 声明。 特権の最終的なスコープは、適用されたすべてのロールのすべての特権の組み合わせです。 ユーザーが権限を付与に直接かつてのユーザーアカウント、または権限を付与する。

ユーザーがデフォルトの役割を応用したユーザーログインします。 デフォルトのロールを設定するには、 [SET DEFAULT ROLE](misc.md#set-default-role-statement) 文または [ALTER USER](alter.md#alter-user-statement) 声明。

ロールを取り消すには、 [REVOKE](revoke.md) 声明。

ロールを削除するには、 [DROP ROLE](misc.md#drop-role-statement) 声明。 削除されたロールは、付与されたすべてのユーザーおよびロールから自動的に取り消されます。

### 例 {#create-role-examples}

``` sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

この一連のクエリは、ロールを作成します `accountant` それはからデータを読み取る権限を持っています `accounting` データベース。

ユーザーへのロールの付与 `mira`:

``` sql
GRANT accountant TO mira;
```

後の役割が付与されたユーザーが利用できる実行させます。 例えば:

``` sql
SET ROLE accountant;
SELECT * FROM db.*;
```

## CREATE ROW POLICY {#create-row-policy-statement}

を作成します。 [行のフィルタ](../../operations/access-rights.md#row-policy-management) ユーザーがテーブルから読み取ることができます。

### 構文 {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### セクションAS {#create-row-policy-as}

このセクショ

確認方針にアクセスが付与されています。 同じテーブルに適用されるパーミッシブポリシーは、booleanを使用して結合されます `OR` オペレーター ポリシーは既定では許可されています。

制限的な政策アクセス制限を行います。 同じテーブルに適用される制限ポリシーは、ブール値を使用して結合されます `AND` オペレーター

制限ポリシーは、許可フィルターを通過した行に適用されます。 制限的なポリシーを設定しても、許可的なポリシーを設定しない場合、ユーザーはテーブルから行を取得できません。

#### セクションへ {#create-row-policy-to}

セクション内 `TO` ロールとユーザーの混在リストを指定できます。, `CREATE ROW POLICY ... TO accountant, john@localhost`.

キーワード `ALL` 現在のユーザを含むすべてのClickHouseユーザを意味します。 キーワード `ALL EXCEPT` allow toは、すべてのユーザーリストから一部のユーザーを除外します。 `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### 例 {#examples}

-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
-   `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`

## CREATE QUOTA {#create-quota-statement}

を作成します。 [クォータ](../../operations/access-rights.md#quotas-management) ユーザーまたはロールに割り当てることができます。

### 構文 {#create-quota-syntax}

``` sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

### 例 {#create-quota-example}

現在のユーザーに対するクエリの最大数を123か月以内に制限する制約:

``` sql
CREATE QUOTA qA FOR INTERVAL 15 MONTH MAX QUERIES 123 TO CURRENT_USER
```

## CREATE SETTINGS PROFILE {#create-settings-profile-statement}

を作成します。 [設定プロファイル](../../operations/access-rights.md#settings-profiles-management) ユーザーまたはロールに割り当てることができます。

### 構文 {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

# 例 {#create-settings-profile-syntax}

を作成します。 `max_memory_usage_profile` の値と制約を持つ設定プロファイル `max_memory_usage` 設定。 それを割り当てる `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

[元の記事](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
