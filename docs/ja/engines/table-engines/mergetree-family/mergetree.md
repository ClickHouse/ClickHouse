---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 30
toc_title: MergeTree
---

# Mergetree {#table_engines-mergetree}

その `MergeTree` この家族 (`*MergeTree`）最も堅牢なクリックハウステーブルエンジンです。

のエンジン `MergeTree` ファミリは、非常に大量のデータをテーブルに挿入するために設計されています。 のデータが書き込まれ、テーブル部、そのルール適用のための統合のパーツです。 この方法は、挿入時にストレージ内のデータを継続的に書き換えるよりはるかに効率的です。

主な特長:

-   店舗データを整理によりその有効なタイプを利用します。

    これにより、データの検索を高速化する小さなスパース索引を作成できます。

-   パーティションは、 [分割キー](custom-partitioning-key.md) が指定される。

    ClickHouseは、同じ結果を持つ同じデータに対する一般的な操作よりも効果的なパーティションを持つ特定の操作をサポートします。 ClickHouseも自動的に遮断すると、パーティションデータのパーティショニングキーで指定されたクエリ。 この改善するためのクエリ。

-   データ複製サポート。

    の家族 `ReplicatedMergeTree` 表はデータ複製を提供します。 詳細については、 [データ複製](replication.md).

-   データ抜取りサポート。

    必要に応じて、テーブル内のデータサンプリング方法を設定できます。

!!! info "情報"
    その [マージ](../special/merge.md#merge) エンジンはに属しません `*MergeTree` 家族

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

パラメータの詳細については、 [クエリの説明の作成](../../../sql-reference/statements/create.md).

!!! note "メモ"
    `INDEX` は、実験的な機能です [データスキップ索引](#table_engine-mergetree-data_skipping-indexes).

### クエリ句 {#mergetree-query-clauses}

-   `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. その `MergeTree` engineにはパラメータがありません。

-   `PARTITION BY` — The [分割キー](custom-partitioning-key.md).

    月単位でパーティション分割するには、 `toYYYYMM(date_column)` 式、どこ `date_column` 型の日付を持つ列を指定します [日付](../../../sql-reference/data-types/date.md). ここでのパーティション名には、 `"YYYYMM"` フォーマット。

-   `ORDER BY` — The sorting key.

    列または任意の式のタプル。 例えば: `ORDER BY (CounterID, EventDate)`.

-   `PRIMARY KEY` — The primary key if it [ソートキーとは異なります](#choosing-a-primary-key-that-differs-from-the-sorting-key).

    デフォルトでは、プライマリキーはソートキー(プライマリキーで指定)と同じです。 `ORDER BY` 句）。 したがって、ほとんどの場合、別の `PRIMARY KEY` 句。

-   `SAMPLE BY` — An expression for sampling.

    サンプリング式を使用する場合は、主キーに含める必要があります。 例えば: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

-   `TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [ディスクとボリューム間](#table_engine-mergetree-multiple-volumes).

    式には次のものが必要です `Date` または `DateTime` 結果としての列。 例えば:
    `TTL date + INTERVAL 1 DAY`

    ルールのタイプ `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'` 式が満たされている場合(現在の時間に達した場合)、その部分を使用して実行するアクションを指定します。 (`TO DISK 'xxx'`)またはボリュームに (`TO VOLUME 'xxx'`). ルールのデフォルトの種類は削除です (`DELETE`). 複数のルールのリストは指定できますが、複数のルールが存在しないはずです `DELETE` ルール。

    詳細については、 [列とテーブルのttl](#table_engine-mergetree-ttl)

-   `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:

    -   `index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [データ記憶](#mergetree-data-storage).
    -   `index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [データ記憶](#mergetree-data-storage).
    -   `enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` 設定。 バージョン19.11以前は、 `index_granularity` 制限の微粒のサイズのための設定。 その `index_granularity_bytes` 設定の改善ClickHouse性能の選定からデータをテーブルの大きな行(数十、数百人のメガバイト). 大きな行を持つテーブルがある場合は、この設定を有効にしてテーブルの効率を向上させることができます `SELECT` クエリ。
    -   `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1`、その後ZooKeeperは以下のデータを格納します。 詳細については、 [設定の説明](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) で “Server configuration parameters”.
    -   `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` バイトClickHouseを読み込みおよび書き込み、データの保存ディスクの直接のI/Oインターフェース (`O_DIRECT` オプション）。 もし `min_merge_bytes_to_use_direct_io = 0` その後、直接I/Oが無効になります。 デフォルト値: `10 * 1024 * 1024 * 1024` バイト。
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).
    -   `write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don’t turn it off.
    -   `merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.
    -   `storage_policy` — Storage policy. See [複数ブロックデバイスを使用したデータ保存](#table_engine-mergetree-multiple-volumes).

**セクション設定例**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

この例では、月単位でパーティション分割を設定します。

また、ユーザーidによるハッシュとしてサンプリング用の式を設定します。 これにより、それぞれのテーブルのデータを擬似乱数化することができます `CounterID` と `EventDate`. を定義した場合 [SAMPLE](../../../sql-reference/statements/select.md#select-sample-clause) 句データを選択すると、ClickHouseはユーザーのサブセットに対して均等に擬似乱数データサンプルを返します。

その `index_granularity` 8192がデフォルト値であるため、設定は省略できます。

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

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

-   `date-column` — The name of a column of the [日付](../../../sql-reference/data-types/date.md) タイプ。 ClickHouseを自動でパーティション月に基づきます。 パーティション名は `"YYYYMM"` フォーマット。
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [タプル()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” インデックスの。 値8192は、ほとんどのタスクに適しています。

**例えば**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

その `MergeTree` エンジンは、メインエンジン構成方法について上記の例と同様に構成される。
</details>

## データ記憶 {#mergetree-data-storage}

テーブルのデータ部品の分別によりその有効なタイプを利用します。

データがテーブルに挿入されると、別々のデータパーツが作成され、それぞれが主キーで辞書式に並べ替えられます。 たとえば、プライマリキーが次の場合 `(CounterID, Date)` パーツ内のデータは `CounterID`、およびそれぞれの中で `CounterID`、それは順序付けられます `Date`.

データに属する別のパーティションが分離の異なる部品です。 その背景にclickhouse合併しデータのパーツを効率的に保管します。 パーツに属する別のパーティションがないます。 マージメカニズムは、同じ主キーを持つすべての行が同じデータ部分にあることを保証しません。

各データ部分は、論理的に顆粒に分割されます。 顆粒は、データを選択するときにclickhouseが読み取る最小の不可分のデータセットです。 clickhouseは行または値を分割しないため、各granule粒には常に整数の行が含まれます。 顆粒の最初の行は、行の主キーの値でマークされます。 各データ、clickhouseを作成しインデックスファイルを格納するのです。 各列について、主キーにあるかどうかにかかわらず、clickhouseには同じマークも格納されます。 これらのマークまたはデータを見つの直列のファイルです。

微粒のサイズはによって制限されます `index_granularity` と `index_granularity_bytes` テーブルエンジンの設定。 微粒の列の数はで置きます `[1, index_granularity]` 行のサイズに応じた範囲。 顆粒のサイズは超えることができます `index_granularity_bytes` 単一行のサイズが設定の値より大きい場合。 この場合、顆粒のサイズは行のサイズに等しくなります。

## クエリの主キーとインデックス {#primary-keys-and-indexes-in-queries}

を取る `(CounterID, Date)` 例として主キー。 この場合、並べ替えとインデックスは次のように示されます:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

デー:

-   `CounterID in ('a', 'h')`、サーバーはマークの範囲のデータを読み取ります `[0, 3)` と `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3`、サーバーはマークの範囲のデータを読み取ります `[1, 3)` と `[7, 8)`.
-   `Date = 3`、サーバーは、マークの範囲内のデータを読み取ります `[1, 10]`.

上記の例としては常に使用するのがより効果的指標により、フルスキャン！

に乏指数で追加するデータを読み込みます。 主キーの単一の範囲を読み取るとき `index_granularity * 2` 余分な列の各データブロック読み取ることができます。

疎指標できる作業は非常に多くのテーブル行において、多くの場合、指数はコンピュータのアプリです。

ClickHouseは一意の主キーを必要としません。 同じ主キーで複数の行を挿入できます。

### 主キーの選択 {#selecting-the-primary-key}

主キーの列数は明示的に制限されていません。 データ構造によっては、主キーに多かれ少なかれ列を含めることができます。 この:

-   インデックスのパフォーマン

    プライマリキーが `(a, b)` 次に、別の列を追加します `c` 次の条件が満たされるとパフォーマンスが向上します:

    -   列に条件があるクエリがあります `c`.
    -   長いデータ範囲(数倍長い `index_granularity`)の値が同じである場合 `(a, b)` 一般的です。 言い換えれば、別の列を追加すると、非常に長いデータ範囲をスキップできます。

-   データ圧縮を改善する。

    ClickHouseは主キーでデータをソートするので、一貫性が高いほど圧縮率が高くなります。

-   追加的なロジックが統合データ部分の [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) と [SummingMergeTree](summingmergetree.md) エンジン

    この場合、それは指定することは理にかなって *ソートキー* これは主キーとは異なります。

長いprimary keyに悪影響を及ぼす可能性は、挿入性能やメモリ消費が別の列に主キーに影響を与えないclickhouse性能 `SELECT` クエリ。

### ソートキーとは異なる主キーの選択 {#choosing-a-primary-key-that-differs-from-the-sorting-key}

ソートキー（データ部分の行をソートする式）とは異なる主キー（各マークのインデックスファイルに書き込まれる値を持つ式）を指定することができます。 この場合、主キー式タプルは、並べ替えキー式タプルのプレフィックスである必要があります。

この機能は、 [SummingMergeTree](summingmergetree.md) と
[ﾂつｨﾂ姪“ﾂつ”ﾂ債ﾂづｭﾂつｹ](aggregatingmergetree.md) テーブルエンジン。 これらのエンジンを使用する一般的なケースでは、テーブルには二種類の列があります: *寸法* と *対策*. 典型的なクエリは、任意のメジャー列の値を集計します `GROUP BY` そして次元によるろ過。 SummingMergeTreeとAggregatingMergeTreeは、並べ替えキーの同じ値を持つ行を集計するので、すべての次元を追加するのが自然です。 その結果、キー式は長い列のリストで構成され、このリストは新しく追加されたディメンションで頻繁に更新される必要があります。

この場合、主キーにいくつかの列だけを残して、効率的な範囲スキャンを提供し、残りのディメンション列を並べ替えキータプルに追加することが理に

[ALTER](../../../sql-reference/statements/alter.md) 新しい列がテーブルとソートキーに同時に追加されると、既存のデータパーツを変更する必要がないため、ソートキーの操作は軽量です。 古いソートキーは新しいソートキーの接頭辞であり、新しく追加された列にデータがないため、データはテーブル変更の時点で古いソートキーと新しいソートキーの両方

### クエリでの索引とパーティションの使用 {#use-of-indexes-and-partitions-in-queries}

のために `SELECT` ClickHouseは、インデックスを使用できるかどうかを分析します。 インデックスが使用できるのは、 `WHERE/PREWHERE` 句には、等式または不等式の比較演算を表す式（連結要素のいずれかとして、または完全に）があります。 `IN` または `LIKE` 主キーまたはパーティショニングキーに含まれる列または式、またはこれらの列の特定の部分反復関数、またはこれらの式の論理関係に固定プレフィッ

したがって、主キーの一つまたは複数の範囲でクエリをすばやく実行することができます。 この例では、特定のトラッキングタグ、特定のタグおよび日付範囲、特定のタグおよび日付、日付範囲を持つ複数のタグなどに対して実行すると、クエ

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

確認clickhouseできるとの利用時の走行クエリに対して、使用の設定 [force\_index\_by\_date](../../../operations/settings/settings.md#settings-force_index_by_date) と [force\_primary\_key](../../../operations/settings/settings.md).

の分割による月で読み込みのみこれらのデータブロックを含むからスピーチへのマークの範囲内で適切に取扱います。 この場合、データブロックには多くの日付（月全体まで）のデータが含まれることがあります。 ブロック内では、データは主キーによってソートされます。 このため、主キープレフィックスを指定しない日付条件のみを持つクエリを使用すると、単一の日付よりも多くのデータが読み取られます。

### 部分的に単調な主キーに対するインデックスの使用 {#use-of-index-for-partially-monotonic-primary-keys}

たとえば、月の日数を考えてみましょう。 彼らは形成する [単調系列](https://en.wikipedia.org/wiki/Monotonic_function) 一ヶ月のために、しかし、より長期間単調ではありません。 これは部分的に単調なシーケンスです。 ユーザーが部分的に単調な主キーを持つテーブルを作成する場合、ClickHouseは通常どおりスパースインデックスを作成します。 ユーザーがこの種類のテーブルからデータを選択すると、ClickHouseはクエリ条件を分析します。 ユーザーは、インデックスの二つのマークの間のデータを取得したいと、これらのマークの両方が一ヶ月以内に落ちる場合、それはクエリとインデックスマーク

クエリパラメーターの範囲内の主キーの値が単調順序を表さない場合、clickhouseはインデックスを使用できません。 この場合、clickhouseはフルスキャン方式を使用します。

ClickHouseは、月シーケンスの日数だけでなく、部分的に単調なシーケンスを表すプライマリキーについても、このロジックを使用します。

### データスキップインデックス(実験) {#table_engine-mergetree-data_skipping-indexes}

インデックス宣言は、次の列セクションにあります `CREATE` クエリ。

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

からのテーブルの場合 `*MergeTree` 家族データの飛び指標を指定できます。

これらのインデックスは、ブロックの指定された式に関する情報を集約します。 `granularity_value` 微粒（微粒のサイズはを使用して指定されます `index_granularity` テーブルエンジンの設定）。 次に、これらの集約は `SELECT` ディスクから読み取るデータの量を減らすためのクエリ `where` クエリは満たされません。

**例えば**

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

この例のインデックスをclickhouseで使用すると、次のクエリでディスクから読み取るデータの量を減らすことができます:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### 使用可能なインデックスの種類 {#available-types-of-indices}

-   `minmax`

    指定された式の極値を格納します(式が指定されている場合 `tuple` そして、それは各要素のための極端をの貯えます `tuple`)を使用して保存情報の飛びブロックのようなデータは、その有効なタイプを利用します。

-   `set(max_rows)`

    指定された式の一意の値を格納します。 `max_rows` 行, `max_rows=0` 意味 “no limits”). この値を使用して、 `WHERE` 式はデータブロックでは充足可能ではありません。

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    店a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) これには、データブロックのすべてのngramsが含まれます。 文字列でのみ動作します。 の最適化に使用することができます `equals`, `like` と `in` 式。

    -   `n` — ngram size,
    -   `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    -   `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
    -   `random_seed` — The seed for Bloom filter hash functions.

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    同じように `ngrambf_v1` しかし、ngramsの代わりにトークンを格納します。 トークンは、英数字以外の文字で区切られた順序です。

-   `bloom_filter([false_positive])` — Stores a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) 指定された列の場合。

    任意 `false_positive` パラメーターは、フィルターから偽陽性の応答を受信する確率です。 可能な値:(0,1)。 デフォルト値:0.025.

    対応データ型: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`.

    次の関数はそれを使用できます: [等しい](../../../sql-reference/functions/comparison-functions.md), [notEquals](../../../sql-reference/functions/comparison-functions.md), [で](../../../sql-reference/functions/in-functions.md), [notIn](../../../sql-reference/functions/in-functions.md), [持っている](../../../sql-reference/functions/array-functions.md).

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### 機能サポート {#functions-support}

の条件 `WHERE` clauseには、列で操作する関数の呼び出しが含まれます。 列がインデックスの一部である場合、ClickHouseは関数の実行時にこのインデックスを使用しようとします。 ClickHouse支援の異なるサブセットの機能を使用。

その `set` indexは、すべての関数で使用できます。 他のインデックスの関数サブセットを以下の表に示します。

| 関数(演算子)/インデックス                                                                                   | 主キー | minmax | ngrambf\_v1 | tokenbf\_v1 | bloom\_filter |
|-------------------------------------------------------------------------------------------------------------|--------|--------|-------------|-------------|---------------|
| [equals(=,==))](../../../sql-reference/functions/comparison-functions.md#function-equals)                   | ✔      | ✔      | ✔           | ✔           | ✔             |
| [notEquals(!=, \<\>)](../../../sql-reference/functions/comparison-functions.md#function-notequals)          | ✔      | ✔      | ✔           | ✔           | ✔             |
| [のように](../../../sql-reference/functions/string-search-functions.md#function-like)                       | ✔      | ✔      | ✔           | ✗           | ✗             |
| [notLike](../../../sql-reference/functions/string-search-functions.md#function-notlike)                     | ✔      | ✔      | ✔           | ✗           | ✗             |
| [startsWith](../../../sql-reference/functions/string-functions.md#startswith)                               | ✔      | ✔      | ✔           | ✔           | ✗             |
| [エンドスウィス](../../../sql-reference/functions/string-functions.md#endswith)                             | ✗      | ✗      | ✔           | ✔           | ✗             |
| [マルチセアチャンネル](../../../sql-reference/functions/string-search-functions.md#function-multisearchany) | ✗      | ✗      | ✔           | ✗           | ✗             |
| [で](../../../sql-reference/functions/in-functions.md#in-functions)                                         | ✔      | ✔      | ✔           | ✔           | ✔             |
| [notIn](../../../sql-reference/functions/in-functions.md#in-functions)                                      | ✔      | ✔      | ✔           | ✔           | ✔             |
| [less(\<)](../../../sql-reference/functions/comparison-functions.md#function-less)                          | ✔      | ✔      | ✗           | ✗           | ✗             |
| [グレーター(\>)](../../../sql-reference/functions/comparison-functions.md#function-greater)                 | ✔      | ✔      | ✗           | ✗           | ✗             |
| [lessOrEquals(\<=)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)         | ✔      | ✔      | ✗           | ✗           | ✗             |
| [greaterOrEquals(\>=)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals)   | ✔      | ✔      | ✗           | ✗           | ✗             |
| [空](../../../sql-reference/functions/array-functions.md#function-empty)                                    | ✔      | ✔      | ✗           | ✗           | ✗             |
| [notEmpty](../../../sql-reference/functions/array-functions.md#function-notempty)                           | ✔      | ✔      | ✗           | ✗           | ✗             |
| ハストケンcity in germany                                                                                   | ✗      | ✗      | ✗           | ✔           | ✗             |

Ngramサイズより小さい定数引数を持つ関数は、 `ngrambf_v1` クエリの最適化のため。

ブルでは偽陽性一致すので、 `ngrambf_v1`, `tokenbf_v1`、と `bloom_filter` インデックスは、関数の結果がfalseであると予想されるクエリの最適化には使用できません。:

-   最適化することができる:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   最適化できません:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`

## 同時データアクセス {#concurrent-data-access}

同時テーブルアクセスでは、マルチバージョンを使用します。 つまり、テーブルが同時に読み取られて更新されると、クエリ時に現在のパーツのセットからデータが読み取られます。 長いロックはありません。 挿入は読み取り操作の方法では得られません。

テーブルからの読み取りは自動的に並列化されます。

## 列とテーブルのttl {#table_engine-mergetree-ttl}

値の存続期間を決定します。

その `TTL` 句は、テーブル全体と個々の列ごとに設定することができます。 テーブルレベルのTTLで指定した論理の自動移動のデータディスクの間とします。

式は評価する必要があります [日付](../../../sql-reference/data-types/date.md) または [DateTime](../../../sql-reference/data-types/datetime.md) データ型。

例えば:

``` sql
TTL time_column
TTL time_column + interval
```

定義する `interval`、使用 [時間間隔](../../../sql-reference/operators.md#operators-datetime) 演算子。

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### 列ttl {#mergetree-column-ttl}

列の値が期限切れになると、clickhouseは列のデータ型の既定値に置き換えます。 すべてのカラム値のデータ部分を切clickhouse削除するこのカラムからのデータにファイルシステム.

その `TTL` キー列には句を使用できません。

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

既存のテーブルの列にttlを追加する

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

列のttlの変更

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### テーブルttl {#mergetree-table-ttl}

テーブルでの表現の除去に終了しました列、複数の表現を自動で部品の移動と [ディスク](#table_engine-mergetree-multiple-volumes). 時テーブルの行の有効期間ClickHouseをすべて削除して対応さい。 部品移動フィーチャの場合、部品のすべての行が移動式の基準を満たしている必要があります。

``` sql
TTL expr [DELETE|TO DISK 'aaa'|TO VOLUME 'bbb'], ...
```

TTLルールのタイプは、各TTL式に従います。 これは、式が満たされると実行されるアクションに影響します（現在の時間に達します):

-   `DELETE` 削除行を終了しました(デフォルトアクション);
-   `TO DISK 'aaa'` -ディスクに部品を移動 `aaa`;
-   `TO VOLUME 'bbb'` -ディスクに部品を移動 `bbb`.

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

テーブルのttlの変更

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**データの削除**

データ切れのttlを取り除きclickhouse合併しデータの部品です。

時clickhouseるデータの期間は終了しましたので、行offスケジュール内スケジュールする必要がありません。 このようなマージの頻度を制御するには、次のように設定します `merge_with_ttl_timeout`. 値が低すぎる場合は、多くのリソースを消費する可能性のある多くのオフスケジュールマージを実行します。

あなたが実行する場合 `SELECT` 期限切れのデータを取得できます。 それを避けるために、を使用 [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) 前にクエリ `SELECT`.

## 複数ブロックデバイスを使用したデータ保存 {#table_engine-mergetree-multiple-volumes}

### 導入 {#introduction}

`MergeTree` 家族のテーブルエンジンでデータを複数のブロックデバイス たとえば、特定のテーブルのデータが暗黙的に分割されている場合に便利です “hot” と “cold”. 最新のデータは定期的に要求されますが、必要な領域はわずかです。 それどころか、fat-tailed履歴データはまれに要求される。 複数のディスクが使用可能な場合は、 “hot” データは高速ディスク(たとえば、NVMe Ssdまたはメモリ内)にあります。 “cold” データ-比較的遅いもの（例えば、HDD）。

データ部分は最低の移動可能な単位のためのです `MergeTree`-エンジンテーブル。 ある部分に属するデータは、あるディスクに格納されます。 データ部分は背景のディスクの間で（ユーザーの設定に従って）、またによって動かすことができます [ALTER](../../../sql-reference/statements/alter.md#alter_move-partition) クエリ。

### 条件 {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [パス](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) サーバー設定。
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

の名称を記載することから、システムテーブル, [システム。ストレージ\_policies](../../../operations/system-tables.md#system_tables-storage_policies) と [システム。ディスク](../../../operations/system-tables.md#system_tables-disks). テーブ `storage_policy` の設定 `MergeTree`-エンジン家族のテーブル。

### 設定 {#table_engine-mergetree-multiple-volumes_configure}

ディスク、ボリューム、およびストレージポリシーは、 `<storage_configuration>` メインファイルのいずれかのタグ `config.xml` または、 `config.d` ディレクトリ。

構成の構造:

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

ストレージポリシ:

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
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume’s disks.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically start to move on the next volume if any (by default, 0.1).

Cofigurationの例:

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

与えられた例では、 `hdd_in_order` ポリシーの実装 [ラウンドロビン](https://en.wikipedia.org/wiki/Round-robin_scheduling) アプローチ。 したがって、このポリシ (`single`)データパーツは、すべてのディスクに循環順序で格納されます。 こうした政策れぞれの知見について学ぶとともに有が複数ある場合は同様のディスク搭載のシステムがRAIDな設定を行います。 個々のディスクドライブはそれぞれ信頼できないため、複製係数が3以上になるように補正する必要があることに注意してください。

システムで使用可能なディスクの種類が異なる場合, `moving_from_ssd_to_hdd` ポリシーは代わりに使用できます。 ボリューム `hot` SSDディスクで構成されています (`fast_ssd`このボリュームに格納できるパーツの最大サイズは1GBです。 サイズが1GBより大きいすべての部品はで直接貯えられます `cold` HDDディスクを含むボリューム `disk1`.
また、一度ディスク `fast_ssd` 80%以上によって満たされて得ます、データはに移ります `disk1` 背景プロセ

ストレージポリシー内のボリューム列挙の順序は重要です。 ボリュームがオーバーフィルされると、データは次のものに移動されます。 ディスク列挙の順序は、データが順番に格納されるため、重要です。

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

その `default` ストレージポリシーは、ボリュームを一つだけ使用することを意味します。 `<path>`. テーブルを作成すると、そのストレージポリシーは変更できません。

### 詳細 {#details}

の場合 `MergeTree` テーブル、データがあるディスクには、異なる方法:

-   挿入の結果として (`INSERT` クエリ）。
-   バックグラウンドマージ時 [突然変異](../../../sql-reference/statements/alter.md#alter-mutations).
-   別のレプリカか
-   仕切りの凍結の結果として [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter.md#alter_freeze-partition).

すべてのこれらの場合を除き、突然変異とパーティションの凍結は、一部が保存され、大量のディスクに保存政策:

1.  パートを格納するのに十分なディスク領域を持つ最初のボリューム(定義の順序で) (`unreserved_space > current_part_size`)特定のサイズの部品を格納することができます (`max_data_part_size_bytes > current_part_size`)が選択されます。
2.  このボリューム内では、前のデータチャンクを格納するために使用されたディスクに続くディスクが選択され、パーツサイズよりも空き領域が多くなり (`unreserved_space - keep_free_space_bytes > current_part_size`).

フードの下で、突然変異および仕切りの凍結は利用します [ハードリンク](https://en.wikipedia.org/wiki/Hard_link). ハードリンクとディスクには対応していないため、この場合、パーツの保管と同じディスクの初期ます。

バックグラウンドでは、空き領域の量に基づいて部品がボリューム間で移動されます (`move_factor` パラメータ)順序に従って、設定ファイルでボリュームが宣言されます。
データは最後のものから最初のものに転送されません。 システムテーブルを使用できる [システム。part\_log](../../../operations/system-tables.md#system_tables-part-log) (フィールド `type = MOVE_PART`） [システム。パーツ](../../../operations/system-tables.md#system_tables-parts) (フィールド `path` と `disk`）背景の動きを監視する。 また、詳細な情報はサーバーログに記載されています。

ユーザーの力で移動中の一部またはパーティションから量別のクエリ [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter.md#alter_move-partition)、バックグラウンド操作のすべての制限が考慮されます。 クエリは単独で移動を開始し、バックグラウンド操作が完了するのを待機しません。 十分な空き領域がない場合、または必要な条件のいずれかが満たされない場合、ユーザーはエラーメッセージが表示されます。

データの移動はデータの複製を妨げません。 そのため、異なる保管方針を指定することができ、同じテーブルの異なるレプリカ.

バックグラウンドマージと突然変異の完了後、古い部分は一定時間後にのみ削除されます (`old_parts_lifetime`).
この間、他のボリュームやディスクに移動されることはありません。 したがって、部品が最終的に取り外されるまで、それらはまだ占有ディスクスペースの評価のために考慮される。

[元の記事](https://clickhouse.tech/docs/ru/operations/table_engines/mergetree/) <!--hide-->
