---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 30
toc_title: "\u30E1\u30EB\u30B2\u30C4\u30EA\u30FC"
---

# メルゲツリー {#table_engines-mergetree}

その `MergeTree` この家族のエンジンそして他のエンジン (`*MergeTree`）は、最も堅牢なClickHouseテーブルエンジンです。

のエンジン `MergeTree` ファミリは、非常に大量のデータをテーブルに挿入するために設計されています。 のデータが書き込まれ、テーブル部、そのルール適用のための統合のパーツです。 この方法は効率よく継続的に書き換えのデータストレージ中をサポートしていません。

主な特長:

-   主キ

    これを作成できる小型疎指標を見つけるデータを高速に行います。

-   この場合、 [分割キー](custom-partitioning-key.md) が指定される。

    ClickHouseは、同じ結果を持つ同じデータに対する一般的な操作よりも効果的なパーティションを持つ特定の操作をサポートします。 ClickHouseも自動的に遮断すると、パーティションデータのパーティショニングキーで指定されたクエリ。 この改善するためのクエリ。

-   データ複製のサポート。

    の家族 `ReplicatedMergeTree` テーブルを提供データレプリケーション. 詳細については、 [データ複製](replication.md).

-   データサンプリングです。

    必要に応じて、テーブル内でデータサンプリング方法を設定できます。

!!! info "情報"
    その [マージ](../special/merge.md#merge) エンジンはに属しません `*MergeTree` 家族だ

## テーブルの作成 {#table_engine-mergetree-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
```

パラメータの説明については、 [クエリの説明の作成](../../../sql-reference/statements/create.md).

!!! note "注"
    `INDEX` 実験的な機能です。 [データスキップ索引](#table_engine-mergetree-data_skipping-indexes).

### クエリ句 {#mergetree-query-clauses}

-   `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. その `MergeTree` エンジンにはパラメータがない。

-   `PARTITION BY` — The [分割キー](custom-partitioning-key.md).

    月によるパーティション分割の場合は、 `toYYYYMM(date_column)` 式、ここで `date_column` 型の日付を持つ列です [日付](../../../sql-reference/data-types/date.md). ここでのパーティション名は `"YYYYMM"` 形式。

-   `ORDER BY` — The sorting key.

    列または任意の式のタプル。 例: `ORDER BY (CounterID, EventDate)`.

-   `PRIMARY KEY` — The primary key if it [ソートキーとは異なります](#choosing-a-primary-key-that-differs-from-the-sorting-key).

    デフォルトでは、プライマリキーはソートキーと同じです。 `ORDER BY` 節）。 したがって、ほとんどの場合、別の `PRIMARY KEY` 句。

-   `SAMPLE BY` — An expression for sampling.

    サンプリング式を使用する場合は、主キーにそれを含める必要があります。 例: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

-   `TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [ディスクとボリューム間](#table_engine-mergetree-multiple-volumes).

    式には次のものが必要です `Date` または `DateTime` 結果としての列。 例:
    `TTL date + INTERVAL 1 DAY`

    ルールのタイプ `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'` 式が満たされた場合(現在の時刻に達した場合)、パートに対して実行されるアクションを指定します:期限切れの行の削除、パートの移動(パート内のすべての (`TO DISK 'xxx'`）またはボリュームに (`TO VOLUME 'xxx'`). ルールの既定の種類は削除です (`DELETE`). リストに複数のルール指定がありませんよ `DELETE` ルール

    詳細は、を参照してください [列および表のTTL](#table_engine-mergetree-ttl)

-   `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:

    -   `index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [データ保存](#mergetree-data-storage).
    -   `index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [データ保存](#mergetree-data-storage).
    -   `enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` 設定。 バージョン19.11以前は `index_granularity` 制限の微粒のサイズのための設定。 その `index_granularity_bytes` 設定の改善ClickHouse性能の選定からデータをテーブルの大きな行(数十、数百人のメガバイト). いテーブルの大きな行きのこの設定のテーブルの効率化 `SELECT` クエリ。
    -   `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1` その飼育係の店が少ない。 詳細については、を参照してください [設定の説明](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) で “Server configuration parameters”.
    -   `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` バイト、ClickHouseは直接入出力インターフェイスを使用して記憶域ディスクにデータを読み、書きます (`O_DIRECT` オプション）。 もし `min_merge_bytes_to_use_direct_io = 0` その後、直接I/Oが無効になります。 デフォルト値: `10 * 1024 * 1024 * 1024` バイト数。
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).
    -   `write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don't turn it off.
    -   `merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.
    -   `storage_policy` — Storage policy. See [複数のブロックデバイスのためのデータ保存](#table_engine-mergetree-multiple-volumes).

**セクション設定例**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

この例では、月別にパーティション分割を設定します。

また、ユーザIDによってハッシュとしてサンプリングする式を設定します。 これにより、各テーブルのデータを擬似乱数化することができます `CounterID` と `EventDate`. を定義すると [SAMPLE](../../../sql-reference/statements/select/sample.md#select-sample-clause) 句データを選択すると、ClickHouseはユーザーのサブセットに対して均等な擬似乱数データサンプルを返します。

その `index_granularity` 8192がデフォルト値であるため、設定を省略できます。

<details markdown="1">

<summary>推奨されていません法テーブルを作成する</summary>

!!! attention "注意"
    用途では使用しないでください方法で新規プロジェクト. 可能であれば、古いプロジェクトを上記の方法に切り替えます。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree()パラメータ**

-   `date-column` — The name of a column of the [日付](../../../sql-reference/data-types/date.md) タイプ。 ClickHouseを自動でパーティション月に基づきます。 パーティション名は `"YYYYMM"` 形式。
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [タプル()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” インデックスの。 値8192は、ほとんどのタスクに適しています。

**例**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

その `MergeTree` エンジンは、メインエンジンの構成方法については、上記の例と同様に構成されています。
</details>

## データ保存 {#mergetree-data-storage}

テーブルのデータ部品の分別によりその有効なタイプを利用します。

データがテーブルに挿入されると、個別のデータパーツが作成され、それぞれが主キーで辞書順に並べ替えられます。 たとえば、主キーが `(CounterID, Date)` パーツ内のデータは次のようにソートされます `CounterID`、およびそれぞれの中 `CounterID`、それは順序付けられます `Date`.

データに属する別のパーティションが分離の異なる部品です。 背景では、ClickHouseはより有効な貯蔵のためのデータ部分を併合する。 異なる区画に属する部分はマージされません。 マージ機構では、同じ主キーを持つすべての行が同じデータ部分に含まれることは保証されません。

各データ部分は論理的にグラニュールに分割されます。 顆粒は、データを選択するときにClickHouseが読み取る最小の不可分データセットです。 ClickHouseは行や値を分割しないため、各グラニュールには常に整数の行が含まれます。 顆粒の最初の行には、行の主キーの値がマークされます。 各データ、ClickHouseを作成しインデックスファイルを格納するのです。 各列について、主キーにあるかどうかにかかわらず、ClickHouseにも同じマークが格納されます。 これらのマークまたはデータを見つの直列のファイルです。

微粒のサイズはによって制限されます `index_granularity` と `index_granularity_bytes` テーブルエンジンの設定。 微粒の列の数はで置きます `[1, index_granularity]` 行のサイズに応じて、範囲。 微粒のサイズは超過できます `index_granularity_bytes` 単一行のサイズが設定の値より大きい場合。 この場合、顆粒のサイズは行のサイズに等しい。

## クエリ内の主キーとインデックス {#primary-keys-and-indexes-in-queries}

を取る `(CounterID, Date)` 例として主キー。 この場合、ソートとインデックスは次のように示すことができます:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

データクエリが指定する場合:

-   `CounterID in ('a', 'h')`、サーバーはマークの範囲のデータを読み取ります `[0, 3)` と `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3`、サーバーはマークの範囲のデータを読み取ります `[1, 3)` と `[7, 8)`.
-   `Date = 3`、サーバは、マークの範囲内のデータを読み取ります `[1, 10]`.

上記の例としては常に使用するのがより効果的指標により、フルスキャン！

に乏指数で追加するデータを読み込みます。 プライマリキーの単一の範囲を読み取るとき `index_granularity * 2` 余分な列の各データブロック読み取ることができます。

疎指標できる作業は非常に多くのテーブル行において、多くの場合、指数はコンピュータのアプリです。

ClickHouseでは一意の主キーは必要ありません。 同じ主キーで複数の行を挿入できます。

### 主キーの選択 {#selecting-the-primary-key}

主キーの列の数は明示的に制限されません。 データ構造によっては、主キーに多かれ少なかれ含めることができます。 これは:

-   索引のパフォーマンスを向上させます。

    主キーが `(a, b)` 次に、別の列を追加します `c` 以下の条件を満たすと、パフォーマンスが向上します:

    -   列に条件を持つクエリがあります `c`.
    -   長いデータ範囲(長いデータ範囲より数倍長い `index_granularity`)と同一の値を持つ。 `(a, b)` 一般的です。 つまり、別の列を追加すると、かなり長いデータ範囲をスキップすることができます。

-   データ圧縮を改善します。

    ClickHouseは主キーによってデータを並べ替えるので、一貫性が高いほど圧縮が良くなります。

-   追加的なロジックが統合データ部分の [折りたたみマージツリー](collapsingmergetree.md#table_engine-collapsingmergetree) と [サミングマーゲツリー](summingmergetree.md) エンジンだ

    この場合、 *ソートキー* これは主キーとは異なります。

長い主キーは挿入のパフォーマンスとメモリ消費に悪影響を及ぼしますが、主キーの追加の列はClickHouseのパフォーマンスには影響しません `SELECT` クエリ。

### 並べ替えキーとは異なる主キーの選択 {#choosing-a-primary-key-that-differs-from-the-sorting-key}

ソートキー(データ部分の行をソートする式)とは異なる主キー(マークごとにインデックスファイルに書き込まれる値を持つ式)を指定することができます。 この場合、主キー式タプルは、並べ替えキー式タプルのプレフィックスでなければなりません。

この機能は、 [サミングマーゲツリー](summingmergetree.md) と
[AggregatingMergeTree](aggregatingmergetree.md) テーブルエンジン。 共通の場合はご利用になられる場合はエンジンのテーブルは、二種類のカラム: *寸法* と *対策*. 典型的なクエリは、任意のメジャー列の値を集計します `GROUP BY` そして次元によるろ過。 SumingmergetreeとAggregatingMergeTreeは並べ替えキーの値が同じ行を集計するため、すべてのディメンションを追加するのは自然です。 その結果、キー式は列の長いリストで構成され、このリストは頻繁に新しく追加されたディメンションで更新する必要があります。

この場合、効率的な範囲スキャンを提供し、残りのディメンション列を並べ替えキータプルに追加する主キーには数列のみを残すことが理にかなって

[ALTER](../../../sql-reference/statements/alter.md) 新しい列がテーブルとソートキーに同時に追加されると、既存のデータ部分を変更する必要がないため、ソートキーの軽量な操作です。 古い並べ替えキーは新しい並べ替えキーのプレフィックスであり、新しく追加された列にデータがないため、テーブルの変更の瞬間に、データは新旧の並べ替え

### クエリでの索引とパーティションの使用 {#use-of-indexes-and-partitions-in-queries}

のために `SELECT` クClickHouseを分析するかどうかの指標を使用できます。 インデックスは、次の場合に使用できます `WHERE/PREWHERE` 句には、等価比較演算または不等比較演算を表す式（連結要素のいずれかとして、または完全に）があります。 `IN` または `LIKE` 主キーまたはパーティショニングキーにある列または式、またはこれらの列の特定の部分的に反復機能、またはこれらの式の論理的関係に固定プレフィッ

したがって、主キーの一つまたは多くの範囲でクエリを迅速に実行することができます。 この例では、特定の追跡タグ、特定のタグと日付範囲、特定のタグと日付、日付範囲を持つ複数のタグなどに対してクエリを実行すると、クエリが高速

次のように構成されたエンジンを見てみましょう:

      ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192

この場合、クエリで:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouseの主キー指標のトリムで不正なデータを毎月パーティショニングキーパンフレット、ホームページの間仕切りする不適切な日。

上記のクエリのインデックスが使用されるときにも複雑な表現です。 テーブルからの読み取りがいを使用した指標できないっぱいたします。

以下の例では、インデックスは使用できません。

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

クエリの実行時にClickHouseがインデックスを使用できるかどうかを確認するには、設定を使用します [force_index_by_date](../../../operations/settings/settings.md#settings-force_index_by_date) と [force_primary_key](../../../operations/settings/settings.md).

の分割による月で読み込みのみこれらのデータブロックを含むからスピーチへのマークの範囲内で適切に取扱います。 この場合、データブロックには多くの日付(月全体まで)のデータが含まれる場合があります。 ブロック内では、データは主キーによってソートされます。 このため、主キープレフィックスを指定しない日付条件のみのクエリを使用すると、単一の日付よりも多くのデータが読み取られます。

### 部分的に単調な主キーのインデックスの使用 {#use-of-index-for-partially-monotonic-primary-keys}

例えば、月の日を考えてみましょう。 それらはaを形作る [単調系列](https://en.wikipedia.org/wiki/Monotonic_function) 一ヶ月のために、しかし、より長期間単調ではありません。 これは部分的に単調なシーケンスです。 ユーザーが部分的に単調な主キーでテーブルを作成する場合、ClickHouseは通常どおりスパースインデックスを作成します。 ユーザーがこの種のテーブルからデータを選択すると、ClickHouseはクエリ条件を分析します。 これは、クエリのパラメータとインデックスマークの間の距離を計算できるためです。

クエリパラメータ範囲内の主キーの値が単調なシーケンスを表していない場合、ClickHouseはインデックスを使用できません。 この場合、ClickHouseはフルスキャン方法を使用します。

ClickHouseは、このロジックを月の日数シーケンスだけでなく、部分的に単調なシーケンスを表す主キーにも使用します。

### データスキップインデックス(実験) {#table_engine-mergetree-data_skipping-indexes}

インデックス宣言は、 `CREATE` クエリ。

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

からのテーブルのため `*MergeTree` 家族データの飛び指標を指定できます。

これらのインデックスは、指定された式に関する情報をブロックに集約します。 `granularity_value` 微粒（微粒のサイズはを使用して指定されます `index_granularity` テーブルエンジンでの設定）。 次に、これらの集計は `SELECT` クエリを削減量のデータから読み込むディスクにより操大きなブロックのデータを `where` クエリが満たされません。

**例**

``` sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

この例のインデックスをClickHouseで使用すると、次のクエリでディスクから読み取るデータ量を減らすことができます:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### 使用可能なインデックスタイプ {#available-types-of-indices}

-   `minmax`

    指定された式の極値を格納します(式が `tuple` の各要素の極値を格納します。 `tuple`)を使用して保存情報の飛びブロックのようなデータは、その有効なタイプを利用します。

-   `set(max_rows)`

    指定された式の一意の値を格納します。 `max_rows` 行, `max_rows=0` つまり “no limits”). 値を使用して、 `WHERE` 式はデータのブロックでは満足できません。

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    ストアa [Bloomフィルタ](https://en.wikipedia.org/wiki/Bloom_filter) これによりngramsからブロックのデータです。 文字列でのみ動作します。 の最適化に使用することができます `equals`, `like` と `in` 式。

    -   `n` — ngram size,
    -   `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    -   `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
    -   `random_seed` — The seed for Bloom filter hash functions.

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    と同じ `ngrambf_v1` しかし、ngramsの代わりにトークンを格納します。 トークンは英数字以外の文字で区切られた配列です。

-   `bloom_filter([false_positive])` — Stores a [Bloomフィルタ](https://en.wikipedia.org/wiki/Bloom_filter) 指定された列の場合。

    任意 `false_positive` パラメータは、フィルタから偽陽性応答を受信する確率です。 可能な値:(0,1)。 既定値は0.025です。

    対応するデータ型: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`.

    以下の関数が使用できます: [等しい](../../../sql-reference/functions/comparison-functions.md), [notEquals](../../../sql-reference/functions/comparison-functions.md), [で](../../../sql-reference/functions/in-functions.md), [ノティン](../../../sql-reference/functions/in-functions.md), [は](../../../sql-reference/functions/array-functions.md).

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### 機能サポート {#functions-support}

の条件 `WHERE` 句には、列で動作する関数の呼び出しが含まれます。 列がインデックスの一部である場合、ClickHouseは関数の実行時にこのインデックスを使用しようとします。 ClickHouse支援の異なるサブセットの機能を使用。

その `set` 索引はすべての機能と使用することができる。 その他のインデックスの関数サブセットを以下の表に示します。

| 関数(演算子)/インデックス                                                                                 | 主キー | minmax | ngrambf_v1 | tokenbf_v1 | bloom_filter name |
|-----------------------------------------------------------------------------------------------------------|--------|--------|-------------|-------------|--------------------|
| [等しい(=,==)](../../../sql-reference/functions/comparison-functions.md#function-equals)                  | ✔      | ✔      | ✔           | ✔           | ✔                  |
| [notEquals(!=, \<\>)](../../../sql-reference/functions/comparison-functions.md#function-notequals)        | ✔      | ✔      | ✔           | ✔           | ✔                  |
| [のように](../../../sql-reference/functions/string-search-functions.md#function-like)                     | ✔      | ✔      | ✔           | ✗           | ✗                  |
| [notLike](../../../sql-reference/functions/string-search-functions.md#function-notlike)                   | ✔      | ✔      | ✔           | ✗           | ✗                  |
| [スタート](../../../sql-reference/functions/string-functions.md#startswith)                               | ✔      | ✔      | ✔           | ✔           | ✗                  |
| [endsWith](../../../sql-reference/functions/string-functions.md#endswith)                                 | ✗      | ✗      | ✔           | ✔           | ✗                  |
| [マルチサーチ](../../../sql-reference/functions/string-search-functions.md#function-multisearchany)       | ✗      | ✗      | ✔           | ✗           | ✗                  |
| [で](../../../sql-reference/functions/in-functions.md#in-functions)                                       | ✔      | ✔      | ✔           | ✔           | ✔                  |
| [ノティン](../../../sql-reference/functions/in-functions.md#in-functions)                                 | ✔      | ✔      | ✔           | ✔           | ✔                  |
| [less(\<)](../../../sql-reference/functions/comparison-functions.md#function-less)                        | ✔      | ✔      | ✗           | ✗           | ✗                  |
| [グレーター(\>)](../../../sql-reference/functions/comparison-functions.md#function-greater)               | ✔      | ✔      | ✗           | ✗           | ✗                  |
| [lessOrEquals(\<=)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)       | ✔      | ✔      | ✗           | ✗           | ✗                  |
| [greaterOrEquals(\>=)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals) | ✔      | ✔      | ✗           | ✗           | ✗                  |
| [空](../../../sql-reference/functions/array-functions.md#function-empty)                                  | ✔      | ✔      | ✗           | ✗           | ✗                  |
| [ノーテンプティ](../../../sql-reference/functions/array-functions.md#function-notempty)                   | ✔      | ✔      | ✗           | ✗           | ✗                  |
| ヘイストケン                                                                                              | ✗      | ✗      | ✗           | ✔           | ✗                  |

Ngramサイズよりも小さい定数引数を持つ関数は、次のように使用できません `ngrambf_v1` クエリ最適化のため。

ブルでは偽陽性一致すので、 `ngrambf_v1`, `tokenbf_v1`,and `bloom_filter` インデックスは、関数の結果がfalseになると予想されるクエリの最適化には使用できません。:

-   最適化できます:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   最適化できない:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`

## 同時データアクセス {#concurrent-data-access}

同時テーブルアクセスには、マルチバージョン管理を使用します。 つまり、テーブルの読み取りと更新が同時に行われると、クエリの時点で現在の部分のセットからデータが読み取られます。 長いロックはありません。 挿入は読み取り操作の邪魔になりません。

テーブルからの読み取りは自動的に並列化されます。

## 列および表のTTL {#table_engine-mergetree-ttl}

値の有効期間を決定します。

その `TTL` 句は、テーブル全体および個々の列ごとに設定できます。 テーブルレベルのTTLで指定した論理の自動移動のデータディスクの間とします。

表現を行い、評価しなければならなす [日付](../../../sql-reference/data-types/date.md) または [DateTime](../../../sql-reference/data-types/datetime.md) データ型。

例:

``` sql
TTL time_column
TTL time_column + interval
```

定義するには `interval`,use [時間間隔](../../../sql-reference/operators/index.md#operators-datetime) 演算子。

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### 列TTL {#mergetree-column-ttl}

列の値が期限切れになると、ClickHouseは列のデータ型の既定値に置き換えます。 データ部分のすべての列の値が期限切れになった場合、ClickHouseはファイルシステム内のデータ部分からこの列を削除します。

その `TTL` 句はキー列には使用できません。

例:

TTLを使用したテーブルの作成

``` sql
CREATE TABLE example_table
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

既存のテーブルの列へのTTLの追加

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

列のTTLを変更する

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### テーブルTTL {#mergetree-table-ttl}

テーブルでの表現の除去に終了しました列、複数の表現を自動で部品の移動と [ディスクまた](#table_engine-mergetree-multiple-volumes). 表の行が期限切れになると、ClickHouseは対応するすべての行を削除します。 パーツ移動フィーチャの場合、パーツのすべての行が移動式基準を満たす必要があります。

``` sql
TTL expr [DELETE|TO DISK 'aaa'|TO VOLUME 'bbb'], ...
```

TTL規則のタイプは、各TTL式に従うことができます。 これは、式が満たされた後（現在の時刻に達する）に実行されるアクションに影響します):

-   `DELETE` 削除行を終了しました(デフォルトアクション);
-   `TO DISK 'aaa'` -ディスクへの部分の移動 `aaa`;
-   `TO VOLUME 'bbb'` -ディスクへの部分の移動 `bbb`.

例:

TTLを使用したテーブルの作成

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH [DELETE],
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

テーブルのTTLの変更

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**データの削除**

期限切れのTTLのデータはClickHouseがデータ部分を結合するとき取除かれる。

時ClickHouseるデータの期間は終了しましたので、行offスケジュール内スケジュールする必要がありません。 このようなマージの頻度を制御するには、次のように設定できます `merge_with_ttl_timeout`. 値が小さすぎると、大量のリソースを消費する可能性のある、スケジュール外のマージが多数実行されます。

を実行する場合 `SELECT` クエリと合併はありませんが、できれば終了しました。 それを避けるには、 [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) クエリの前に `SELECT`.

## 複数のブロックデバイスのためのデータ保存 {#table_engine-mergetree-multiple-volumes}

### はじめに {#introduction}

`MergeTree` 家族のテーブルエンジンでデータを複数のブロックデバイス たとえば、特定のテーブルのデータが暗黙的に次のように分割される場合に便利です “hot” と “cold”. 最新のデータは定期的に要求されますが、わずかなスペースしか必要ありません。 逆に、fat-tailed履歴データはほとんど要求されません。 複数のディスクが利用できれば、 “hot” データは高速ディスク(例えば、NVMe Ssdまたはメモリ内)に配置されることがあります。 “cold” データ-比較的遅いもの（例えば、HDD）。

データ部分は最低の移動可能な単位のためのです `MergeTree`-エンジンテーブル。 ある部分に属するデータは、一つのディスクに保存されます。 データパーツは、バックグラウンドで(ユーザー設定に従って)ディスク間で移動することができます。 [ALTER](../../../sql-reference/statements/alter.md#alter_move-partition) クエリ。

### 用語 {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [パス](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) サーバー設定。
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

の名称を記載することから、システムテーブル, [システムストレージポリシー](../../../operations/system-tables.md#system_tables-storage_policies) と [システムディスク](../../../operations/system-tables.md#system_tables-disks). 適用の設定を保存政策のためのテーブルを使用 `storage_policy` 設定の `MergeTree`-エンジン家族のテーブル。

### 設定 {#table_engine-mergetree-multiple-volumes_configure}

ディスク、ボリューム、およびストレージポリシーは、 `<storage_configuration>` メインファイルにタグを付ける `config.xml` または、 `config.d` ディレクトリ。

構成構造:

``` xml
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

-   `<disk_name_N>` — Disk name. Names must be different for all disks.
-   `path` — path under which a server will store data (`data` と `shadow` フォルダ）で終了する必要があります。 ‘/’.
-   `keep_free_space_bytes` — the amount of free disk space to be reserved.

ディスク定義の順序は重要ではありません。

保管方針の設定のマークアップ:

``` xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
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

-   `policy_name_N` — Policy name. Policy names must be unique.
-   `volume_name_N` — Volume name. Volume names must be unique.
-   `disk` — a disk within a volume.
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically start to move on the next volume if any (by default, 0.1).

構成の例:

``` xml
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
    </policies>
    ...
</storage_configuration>
```

与えられた例では、 `hdd_in_order` ポリシーは、 [ラウンドロビン](https://en.wikipedia.org/wiki/Round-robin_scheduling) アプローチ このようにこの方針を定義しみ量 (`single`）、データ部分は円形の順序ですべてのディスクに格納されています。 こうした政策れぞれの知見について学ぶとともに有が複数ある場合は同様のディスク搭載のシステムがRAIDな設定を行います。 個々のディスクドライブは信頼できないため、複製係数が3以上で補う必要がある場合があります。

システムで使用可能なディスクの種類が異なる場合, `moving_from_ssd_to_hdd` 代わりにポリシーを使用できます。 ボリューム `hot` SSDディスクで構成されています (`fast_ssd`)、このボリュームに格納できるパーツの最大サイズは1GBです。 1GBより大きいサイズのすべての部品はで直接貯えられます `cold` HDDディスクを含むボリューム `disk1`.
また、一度ディスク `fast_ssd` 80%以上によって満たされて、データはに移ります得ます `disk1` 背景プロセスによって。

ストレージポリシー内のボリューム列挙の順序は重要です。 ボリュームが満杯になると、データは次のボリュームに移動されます。 データは順番に格納されるため、ディスク列挙の順序も重要です。

作成時にテーブルは、適用の設定を保存方針で:

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

その `default` ストレージポリシーは、一つのボリュームのみを使用することを意味します。 `<path>`. テーブルを作成すると、そのストレージポリシーは変更できません。

### 詳細 {#details}

の場合 `MergeTree` テーブル、データがあるディスクには、異なる方法:

-   挿入の結果として (`INSERT` クエリ）。
-   バックグラウンドマージ中 [突然変異](../../../sql-reference/statements/alter.md#alter-mutations).
-   別のレプリカからダウンロ
-   パーティションの凍結の結果として [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter.md#alter_freeze-partition).

すべてのこれらの場合を除き、突然変異とパーティションの凍結は、一部が保存され、大量のディスクに保存政策:

1.  部品を格納するのに十分なディスク領域を持つ最初のボリューム(定義順) (`unreserved_space > current_part_size`）と指定されたサイズの部分を格納することができます (`max_data_part_size_bytes > current_part_size`）が選ばれる。
2.  このボリューム内では、データの前のチャンクを格納するために使用され、パートサイズよりも空き領域が多いディスクが選択されます (`unreserved_space - keep_free_space_bytes > current_part_size`).

フードの下で、突然変異および仕切りの凍結は利用します [ハードリンク](https://en.wikipedia.org/wiki/Hard_link). ハードリンクとディスクには対応していないため、この場合、パーツの保管と同じディスクの初期ます。

バックグラウンドでは、部品は空き領域の量に基づいてボリューム間で移動されます (`move_factor` パラメータ)ボリュームが設定ファイルで宣言されている順序に従って。
データは、最後のデータから最初のデータに転送されることはありません。 システムテーブル [システムpart_log](../../../operations/system-tables.md#system_tables-part-log) （フィールド `type = MOVE_PART`)と [システム部品](../../../operations/system-tables.md#system_tables-parts) （フィールド `path` と `disk`）背景の動きを監視します。 また、詳細情報はサーバーログに記載されています。

ユーザーの力で移動中の一部またはパーティションから量別のクエリ [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter.md#alter_move-partition) バックグラウンド操作のすべての制限が考慮されます。 クエリは単独で移動を開始し、バックグラウンド操作が完了するまで待機しません。 十分な空き領域がない場合、または必要な条件のいずれかが満たされていない場合、ユーザーはエラーメッセージを表示します。

データの移動は、データの複製を妨げません。 そのため、異なる保管方針を指定することができ、同じテーブルの異なるレプリカ.

バックグラウンドマージと突然変異の完了後、古い部分は一定時間後にのみ削除されます (`old_parts_lifetime`).
この間、他のボリュームまたはディスクには移動されません。 したがって、部品が最終的に除去されるまで、それらは占有されたディスク領域の評価のために考慮される。

[元の記事](https://clickhouse.tech/docs/ru/operations/table_engines/mergetree/) <!--hide-->
