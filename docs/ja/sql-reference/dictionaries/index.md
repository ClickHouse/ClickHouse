---
slug: /ja/sql-reference/dictionaries
sidebar_label: ディクショナリ定義
sidebar_position: 35
---

import SelfManaged from '@site/docs/ja/_snippets/_self_managed_only_no_roadmap.md';
import CloudDetails from '@site/docs/ja/sql-reference/dictionaries/_snippet_dictionary_in_cloud.md';

# ディクショナリ

ディクショナリは、さまざまな種類の参照リストに便利なマッピング (`key -> attributes`) です。

ClickHouseはクエリで使用できるディクショナリを操作するための特別な関数をサポートしています。リファレンステーブルとの `JOIN` よりも、関数を使用してディクショナリを使うほうが簡単かつ効率的です。

ClickHouseは次をサポートしています:

- [一連の関数](../../sql-reference/functions/ext-dict-functions.md)を持つディクショナリ。
- 特定の[一連の関数](../../sql-reference/functions/ym-dict-functions.md)を持つ[埋め込みディクショナリ](#embedded-dictionaries)。

:::tip チュートリアル
ClickHouseでディクショナリを始める場合には、そのトピックをカバーしたチュートリアルがあります。[こちら](/docs/ja/tutorial.md)をご覧ください。
:::

クリックハウスのディクショナリはさまざまなデータソースから追加できます。ディクショナリのソースには、ClickHouseテーブル、ローカルテキストまたは実行ファイル、HTTP(s)リソース、または他のDBMSを使うことができます。詳細は「[ディクショナリソース](#dictionary-sources)」を参照してください。

ClickHouse:

- ディクショナリをRAMに完全または部分的に保存します。
- ディクショナリを定期的に更新し、欠けている値を動的にロードします。つまり、ディクショナリは動的にロードできます。
- xmlファイルまたは[DDLクエリ](../../sql-reference/statements/create/dictionary.md)でディクショナリを作成できます。

ディクショナリの設定は、1つ以上のxmlファイルに配置できます。設定へのパスは、[dictionaries_config](../../operations/server-configuration-parameters/settings.md#dictionaries_config)パラメータで指定します。

ディクショナリは、サーバーの起動時または最初の使用時にロードされますが、それは[dictionaries_lazy_load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)設定に依存します。

[システムテーブルdictionaries](../../operations/system-tables/dictionaries.md#system_tables-dictionaries)には、サーバーで設定されたディクショナリの情報が含まれています。各ディクショナリについて、次の情報を確認できます：

- ディクショナリのステータス。
- 設定パラメータ。
- ディクショナリが正常にロードされてからのRAM使用量やクエリ数などのメトリック。

<CloudDetails />

## DDLクエリを使用したディクショナリの作成 {#creating-a-dictionary-with-a-ddl-query}

ディクショナリは[DDLクエリ](../../sql-reference/statements/create/dictionary.md)を使用して作成できます。DDLで作成したディクショナリは次の利点があるため、推奨される方法です:
- サーバーの設定ファイルに追加のレコードが追加されない
- ディクショナリはテーブルやビューのように第一級のエンティティとして扱える
- ディクショナリテーブル関数ではなく、SELECTを使用して直接データを読み取ることができる
- ディクショナリの名前を簡単に変更できる

## 設定ファイルを使用したディクショナリの作成

:::note
設定ファイルを用いたディクショナリの作成は、ClickHouse Cloudには適用されません。上記のDDLを使用し、ユーザー`default`としてディクショナリを作成してください。
:::

ディクショナリの設定ファイルは次の形式を持ちます:

``` xml
<clickhouse>
    <comment>任意のコンテンツを持つオプション要素。ClickHouseサーバーによって無視されます。</comment>

    <!--オプション要素。置換が含まれたファイル名-->
    <include_from>/etc/metrika.xml</include_from>

    <dictionary>
        <!-- ディクショナリ設定。 -->
        <!-- 設定ファイルには任意の数のディクショナリセクションを含むことができます。 -->
    </dictionary>

</clickhouse>
```

同じファイルで任意の数のディクショナリを[設定](#configuring-a-dictionary)できます。

:::note
小さなディクショナリの値を`SELECT`クエリで記述することで変換できます（[transform](../../sql-reference/functions/other-functions.md)関数を参照）。この機能はディクショナリとは関係ありません。
:::

## ディクショナリの設定

<CloudDetails />

ディクショナリがxmlファイルで設定されている場合、ディクショナリの設定は次の構造を持ちます:

``` xml
<dictionary>
    <name>dict_name</name>

    <structure>
      <!-- 複合キーの設定 -->
    </structure>

    <source>
      <!-- ソースの設定 -->
    </source>

    <layout>
      <!-- メモリレイアウトの設定 -->
    </layout>

    <lifetime>
      <!-- メモリにおけるディクショナリの寿命 -->
    </lifetime>
</dictionary>
```

対応する[DDLクエリ](../../sql-reference/statements/create/dictionary.md)は次の構造です:

``` sql
CREATE DICTIONARY dict_name
(
    ... -- 属性
)
PRIMARY KEY ... -- 複合または単一キーの設定
SOURCE(...) -- ソース設定
LAYOUT(...) -- メモリレイアウトの設定
LIFETIME(...) -- メモリにおけるディクショナリの寿命
```

## メモリにディクショナリを保存する方法

ディクショナリをメモリに保存する方法はさまざまです。

最適な処理速度を提供する[flat](#flat)、[hashed](#hashed)および[complex_key_hashed](#complex_key_hashed)をお勧めします。

キャッシュは潜在的な低パフォーマンスと最適なパラメータの選択が難しいため推奨されません。[cache](#cache)セクションで詳細を読むことができます。

ディクショナリのパフォーマンスを向上させるいくつかの方法があります:

- ディクショナリで操作するための関数を`GROUP BY`の後に呼び出す。
- 抽出する属性を全射的であるとマークします。属性が異なるキーに対応する場合、それを全射と呼びます。つまり、`GROUP BY`がキーで属性値を取得する関数を使用する場合、この関数が`GROUP BY`から自動的に取り出されます。

ClickHouseはエラーがディクショナリで発生した場合に例外を生成します。エラーの例:

- アクセスしようとしたディクショナリがロードできなかった。
- `cached`ディクショナリをクエリする際のエラー。

[system.dictionaries](../../operations/system-tables/dictionaries.md)テーブルでディクショナリのリストとそのステータスを表示できます。

<CloudDetails />

設定は次のようになります:

``` xml
<clickhouse>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- レイアウト設定 -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</clickhouse>
```

対応する[DDLクエリ](../../sql-reference/statements/create/dictionary.md)は次の通りです:

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- レイアウト設定
...
```

レイアウトに`complex-key*`という言葉が含まれていないディクショナリは、[UInt64](../../sql-reference/data-types/int-uint.md)型のキーを持ち、`complex-key*`ディクショナリは複合キー（任意の型）を持ちます。

XMLディクショナリ内の[UInt64](../../sql-reference/data-types/int-uint.md)キーは`<id>`タグで定義します。

設定の例（カラムkey_columnがUInt64型の場合）:
```xml
...
<structure>
    <id>
        <name>key_column</name>
    </id>
...
```

複合`complex`キーのXMLディクショナリは`<key>`タグで定義します。

複合キーの設定例（キーが[String](../../sql-reference/data-types/string.md)型の1要素を持つ場合）:
```xml
...
<structure>
    <key>
        <attribute>
            <name>country_code</name>
            <type>String</type>
        </attribute>
    </key>
...
```

## メモリにディクショナリを保存する方法

- [flat](#flat)
- [hashed](#hashed)
- [sparse_hashed](#sparse_hashed)
- [complex_key_hashed](#complex_key_hashed)
- [complex_key_sparse_hashed](#complex_key_sparse_hashed)
- [hashed_array](#hashed_array)
- [complex_key_hashed_array](#complex_key_hashed_array)
- [range_hashed](#range_hashed)
- [complex_key_range_hashed](#complex_key_range_hashed)
- [cache](#cache)
- [complex_key_cache](#complex_key_cache)
- [ssd_cache](#ssd_cache)
- [complex_key_ssd_cache](#complex_key_ssd_cache)
- [direct](#direct)
- [complex_key_direct](#complex_key_direct)
- [ip_trie](#ip_trie)

### flat

ディクショナリはフラット配列の形でメモリに完全に保存されます。ディクショナリのメモリ使用量はどれくらいですか？それは最大サイズのキーに比例します。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ち、値は`max_array_size`（デフォルトは500,000）に制限されます。ディクショナリ作成時により大きなキーが見つかった場合、ClickHouseは例外をスローし、ディクショナリを作成しません。ディクショナリのフラット配列初期サイズは`initial_array_size`設定（デフォルトは1024）で制御されます。

全ての種類のソースがサポートされています。更新する際には、データ（ファイルまたはテーブルから）は全体で読み込まれます。

この方法は、ディクショナリを保存するためのすべての方法の中で最高のパフォーマンスを提供します。

設定の例:

``` xml
<layout>
  <flat>
    <initial_array_size>50000</initial_array_size>
    <max_array_size>5000000</max_array_size>
  </flat>
</layout>
```

または

``` sql
LAYOUT(FLAT(INITIAL_ARRAY_SIZE 50000 MAX_ARRAY_SIZE 5000000))
```

### hashed

ディクショナリは、ハッシュテーブルの形でメモリに完全に保存されます。ディクショナリは任意の数の要素と識別子を持つことができます。実際には、キーの数は何千万アイテムに達する可能性があります。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。

全ての種類のソースがサポートされています。更新する際には、データ（ファイルまたはテーブルから）が全体として読み込まれます。

設定の例:

``` xml
<layout>
  <hashed />
</layout>
```

または

``` sql
LAYOUT(HASHED())
```

設定の例:

``` xml
<layout>
  <hashed>
    <!-- シャードの数が1を超える（デフォルトは`1`）場合、ディクショナリは並列でデータをロードします。
         1つのディクショナリに大量の要素がある場合に便利です。 -->
    <shards>10</shards>

    <!-- 並列キュー内のブロックのバックログのサイズ。
         並列ロードのボトルネックは再ハッシュであり、そのためスレッドが再ハッシュを実行するために停止しないようにするために
         ある程度のバックログが必要です。
         メモリと速度のバランスでは10000が良いです。
         10e10要素でも全負荷を処理できます。 -->
    <shard_load_queue_backlog>10000</shard_load_queue_backlog>

    <!-- ハッシュテーブルの最大ロードファクタ。高い値では、メモリがより効率的に使用され（メモリが無駄にならない）ますが、
         読み取り/パフォーマンスが低下する可能性があります。
         有効な値: [0.5, 0.99]
         デフォルト: 0.5 -->
    <max_load_factor>0.5</max_load_factor>
  </hashed>
</layout>
```

または

``` sql
LAYOUT(HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

### sparse_hashed

`hashed` に似ていますが、より多くのCPU使用量に対してメモリを少なく使用します。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。

設定の例:

``` xml
<layout>
  <sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </sparse_hashed>
</layout>
```

または

``` sql
LAYOUT(SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

この種類のディクショナリでは、`shards`を使用することも可能で、再びそれは`hashed`に比べて`sparse_hashed`の方がより重要です。なぜなら、`sparse_hashed`はより遅いためです。

### complex_key_hashed

このストレージタイプは、複合[キー](#dictionary-key-and-fields)と共に使用されます。`hashed`に似ています。

設定の例:

``` xml
<layout>
  <complex_key_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_hashed>
</layout>
```

または

``` sql
LAYOUT(COMPLEX_KEY_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

### complex_key_sparse_hashed

このストレージタイプは、複合[キー](#dictionary-key-and-fields)と共に使用されます。[sparse_hashed](#sparse_hashed)に似ています。

設定の例:

``` xml
<layout>
  <complex_key_sparse_hashed>
    <!-- <shards>1</shards> -->
    <!-- <shard_load_queue_backlog>10000</shard_load_queue_backlog> -->
    <!-- <max_load_factor>0.5</max_load_factor> -->
  </complex_key_sparse_hashed>
</layout>
```

または

``` sql
LAYOUT(COMPLEX_KEY_SPARSE_HASHED([SHARDS 1] [SHARD_LOAD_QUEUE_BACKLOG 10000] [MAX_LOAD_FACTOR 0.5]))
```

### hashed_array

ディクショナリは完全にメモリに保存されます。各属性は配列に保存されます。キー属性はハッシュテーブルの形式で保存され、値は属性配列のインデックスです。ディクショナリは任意の数の要素と識別子を持つことができます。実際には、キーの数は何千万アイテムに達する可能性があります。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。

全ての種類のソースがサポートされています。更新する際には、データ（ファイルまたはテーブルから）が全体として読み込まれます。

設定の例:

``` xml
<layout>
  <hashed_array>
  </hashed_array>
</layout>
```

または

``` sql
LAYOUT(HASHED_ARRAY([SHARDS 1]))
```

### complex_key_hashed_array

このストレージタイプは、複合[キー](#dictionary-key-and-fields)と共に使用されます。[hashed_array](#hashed_array)に似ています。

設定の例:

``` xml
<layout>
  <complex_key_hashed_array />
</layout>
```

または

``` sql
LAYOUT(COMPLEX_KEY_HASHED_ARRAY([SHARDS 1]))
```

### range_hashed

ディクショナリは、範囲とそれに対応する値の順序付き配列を持つハッシュテーブルの形でメモリに保存されます。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。
このストレージ方法は、`hashed`と同様に機能し、キーに加えて日付/時間（任意の数値型）の範囲を使用できます。

例: テーブルは各広告主に対する割引を次の形式で含みます:

``` text
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│           123 │          2015-01-16 │        2015-01-31 │   0.25 │
│           123 │          2015-01-01 │        2015-01-15 │   0.15 │
│           456 │          2015-01-01 │        2015-01-15 │   0.05 │
└───────────────┴─────────────────────┴───────────────────┴────────┘
```

日付範囲のサンプルを使用するには、[structure](#dictionary-key-and-fields)で`range_min`と`range_max`要素を定義します。これらの要素は`name`と`type`（`type`が指定されていない場合はデフォルト型であるDateが使用されます）の要素を持つ必要があります。`type`は任意の数値型（Date / DateTime / UInt64 / Int32 / その他）が可能です。

:::note
`range_min`と`range_max`の値は`Int64`型に収まるべきです。
:::

例:

``` xml
<layout>
    <range_hashed>
        <!-- 範囲の重複防止戦略（min/max）。デフォルト: min (範囲の値範囲で最小のminを持つ範囲を返す) -->
        <range_lookup_strategy>min</range_lookup_strategy>
    </range_hashed>
</layout>
<structure>
    <id>
        <name>advertiser_id</name>
    </id>
    <range_min>
        <name>discount_start_date</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>discount_end_date</name>
        <type>Date</type>
    </range_max>
    ...
```

または

``` sql
CREATE DICTIONARY discounts_dict (
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Date,
    amount Float64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'discounts'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(RANGE_HASHED(range_lookup_strategy 'max'))
RANGE(MIN discount_start_date MAX discount_end_date)
```

これらのディクショナリを操作するには、`dictGet`関数に範囲を選択するための追加の引数を渡す必要があります:

``` sql
dictGet('dict_name', 'attr_name', id, date)
```

クエリ例:

``` sql
SELECT dictGet('discounts_dict', 'amount', 1, '2022-10-20'::Date);
```

この関数は、指定された`id`と、渡された日付を含む日付範囲の値を返します。

アルゴリズムの詳細:

- `id`が見つからないか、`id`のための範囲が見つからない場合、その属性の型のデフォルト値を返します。
- 重複する範囲があり、`range_lookup_strategy=min`である場合、最小の`range_min`を持つ一致する範囲を返します。複数の範囲が見つかった場合、最小の`range_max`を持つ範囲を返します。それでも複数の範囲が（同じ`range_min`と`range_max`を持つ複数の範囲があった場合）は、ランダムにその中の一つが返されます。
- 重複する範囲があり、`range_lookup_strategy=max`である場合、最大の`range_min`を持つ一致する範囲を返します。複数の範囲が見つかった場合、最大の`range_max`を持つ範囲を返します。それでも複数の範囲が（同じ`range_min`と`range_max`を持つ複数の範囲があった場合）は、ランダムにその中の一つが返されます。
- `range_max`が`NULL`の場合、範囲は開かれています。`NULL`は最大可能値として扱われます。`range_min`に対しては、`1970-01-01`または`0`（-MAX_INT）が開いた値として使用できます。

設定例:

``` xml
<clickhouse>
    <dictionary>
        ...

        <layout>
            <range_hashed />
        </layout>

        <structure>
            <id>
                <name>Abcdef</name>
            </id>
            <range_min>
                <name>StartTimeStamp</name>
                <type>UInt64</type>
            </range_min>
            <range_max>
                <name>EndTimeStamp</name>
                <type>UInt64</type>
            </range_max>
            <attribute>
                <name>XXXType</name>
                <type>String</type>
                <null_value />
            </attribute>
        </structure>

    </dictionary>
</clickhouse>
```

または

``` sql
CREATE DICTIONARY somedict(
    Abcdef UInt64,
    StartTimeStamp UInt64,
    EndTimeStamp UInt64,
    XXXType String DEFAULT ''
)
PRIMARY KEY Abcdef
RANGE(MIN StartTimeStamp MAX EndTimeStamp)
```

重複する範囲と開いた範囲の設定例:

```sql
CREATE TABLE discounts
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
ENGINE = Memory;

INSERT INTO discounts VALUES (1, '2015-01-01', Null, 0.1);
INSERT INTO discounts VALUES (1, '2015-01-15', Null, 0.2);
INSERT INTO discounts VALUES (2, '2015-01-01', '2015-01-15', 0.3);
INSERT INTO discounts VALUES (2, '2015-01-04', '2015-01-10', 0.4);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-15', 0.5);
INSERT INTO discounts VALUES (3, '1970-01-01', '2015-01-10', 0.6);

SELECT * FROM discounts ORDER BY advertiser_id, discount_start_date;
┌─advertiser_id─┬─discount_start_date─┬─discount_end_date─┬─amount─┐
│             1 │          2015-01-01 │              ᴺᵁᴸᴸ │    0.1 │
│             1 │          2015-01-15 │              ᴺᵁᴸᴸ │    0.2 │
│             2 │          2015-01-01 │        2015-01-15 │    0.3 │
│             2 │          2015-01-04 │        2015-01-10 │    0.4 │
│             3 │          1970-01-01 │        2015-01-15 │    0.5 │
│             3 │          1970-01-01 │        2015-01-10 │    0.6 │
└───────────────┴─────────────────────┴───────────────────┴────────┘

-- RANGE_LOOKUP_STRATEGY 'max'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'max'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- 一致する範囲は1つのみ: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.2 │ -- 2つの範囲が一致、range_min 2015-01-15 (0.2)が2015-01-01 (0.1)より大きい
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.4 │ -- 2つの範囲が一致、range_min 2015-01-04 (0.4)が2015-01-01 (0.3)より大きい
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.5 │ -- 2つの範囲が一致、range_minは等しい、2015-01-15 (0.5)が2015-01-10 (0.6)より大きい
└─────┘

DROP DICTIONARY discounts_dict;

-- RANGE_LOOKUP_STRATEGY 'min'

CREATE DICTIONARY discounts_dict
(
    advertiser_id UInt64,
    discount_start_date Date,
    discount_end_date Nullable(Date),
    amount Float64
)
PRIMARY KEY advertiser_id
SOURCE(CLICKHOUSE(TABLE discounts))
LIFETIME(MIN 600 MAX 900)
LAYOUT(RANGE_HASHED(RANGE_LOOKUP_STRATEGY 'min'))
RANGE(MIN discount_start_date MAX discount_end_date);

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-14')) res;
┌─res─┐
│ 0.1 │ -- 一致する範囲は1つだけ: 2015-01-01 - Null
└─────┘

select dictGet('discounts_dict', 'amount', 1, toDate('2015-01-16')) res;
┌─res─┐
│ 0.1 │ -- 2つの範囲が一致、range_min 2015-01-01 (0.1) が2015-01-15 (0.2)より小さい
└─────┘

select dictGet('discounts_dict', 'amount', 2, toDate('2015-01-06')) res;
┌─res─┐
│ 0.3 │ -- 2つの範囲が一致、range_min 2015-01-01 (0.3) が2015-01-04 (0.4)より小さい
└─────┘

select dictGet('discounts_dict', 'amount', 3, toDate('2015-01-01')) res;
┌─res─┐
│ 0.6 │ -- 2つの範囲が一致、range_minは等しい、2015-01-10 (0.6) が2015-01-15 (0.5)より小さい
└─────┘
```

### complex_key_range_hashed

ディクショナリは、範囲とそれに対応する値の順序付き配列を持つハッシュテーブルの形でメモリに保存されます（[range_hashed](#range_hashed)を参照）。このストレージタイプは複合[キー](#dictionary-key-and-fields)用です。

設定例:

``` sql
CREATE DICTIONARY range_dictionary
(
  CountryID UInt64,
  CountryKey String,
  StartDate Date,
  EndDate Date,
  Tax Float64 DEFAULT 0.2
)
PRIMARY KEY CountryID, CountryKey
SOURCE(CLICKHOUSE(TABLE 'date_table'))
LIFETIME(MIN 1 MAX 1000)
LAYOUT(COMPLEX_KEY_RANGE_HASHED())
RANGE(MIN StartDate MAX EndDate);
```

### cache

ディクショナリは、固定数のセルを持つキャッシュに保存されます。これらのセルには頻繁に使用される要素が含まれます。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。

ディクショナリを検索する際、まずキャッシュが検索されます。データブロックごとに、キャッシュに見つからない、または期限切れのすべてのキーが`SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`を使ってソースから要求されます。受信したデータはキャッシュに書き込まれます。

キーがディクショナリに見つからない場合、キャッシュ更新タスクが作成され、更新キューに追加されます。更新キューの特性は`max_update_queue_size`、`update_queue_push_timeout_milliseconds`、`query_wait_timeout_milliseconds`、`max_threads_for_updates`設定によって制御できます。

キャッシュディクショナリの場合、[lifetime](#refreshing-dictionary-data-using-lifetime)でデータの有効期限を設定できます。セルにデータがロードされてから`lifetime`を超える時間が経過した場合、セルの値は使用されず、キーが期限切れになります。このキーは次回使用時に再要求されます。この動作は`allow_read_expired_keys`設定で構成できます。

これはディクショナリを保存するすべての方法の中で最も効果が低いものです。キャッシュの速度は、設定と使用シナリオに大きく依存します。キャッシュタイプのディクショナリは、ヒット率が十分に高い場合にのみ良好に動作します（推奨99%以上）。[system.dictionaries](../../operations/system-tables/dictionaries.md)テーブルで平均ヒット率を確認できます。

設定`allow_read_expired_keys`が1に設定されている場合（デフォルトは0）、ディクショナリは非同期更新をサポートできます。クライアントがキーを要求し、すべてのキーがキャッシュに存在するが、一部のキーが期限切れになっている場合、ディクショナリはクライアントに期限切れのキーを返し、ソースから非同期に要求されます。

キャッシュの性能を向上させるには、`LIMIT`付きのサブクエリを使用し、ディクショナリと外部に関数を呼び出します。

すべての種類のソースがサポートされています。

設定例:

``` xml
<layout>
    <cache>
        <!-- キャッシュのサイズ、セルの数単位。2の累乗に切り上げられます。 -->
        <size_in_cells>1000000000</size_in_cells>
        <!-- 有効期限切れのキーを読み込むことを許可します。 -->
        <allow_read_expired_keys>0</allow_read_expired_keys>
        <!-- 更新キューの最大サイズ。 -->
        <max_update_queue_size>100000</max_update_queue_size>
        <!-- キューへの更新タスク投入の最大タイムアウト時間（ミリ秒単位）。 -->
        <update_queue_push_timeout_milliseconds>10</update_queue_push_timeout_milliseconds>
        <!-- 更新タスク完了のための最大待機タイムアウト時間（ミリ秒単位）。 -->
        <query_wait_timeout_milliseconds>60000</query_wait_timeout_milliseconds>
        <!-- キャッシュディクショナリの最大更新スレッド数。 -->
        <max_threads_for_updates>4</max_threads_for_updates>
    </cache>
</layout>
```

または

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

十分なキャッシュサイズを設定してください。セル数を選択するには実験が必要です：

1. 各種値を設定します。
2. キャッシュが完全に満たされるまでクエリを実行します。
3. `system.dictionaries`テーブルを使用してメモリ消費量を評価します。
4. 必要なメモリ消費量に達するまでセル数を増減します。

:::note
ClickHouseをソースとして使用しないでください。ランダム読みによるクエリ処理が遅いためです。
:::

### complex_key_cache

このストレージタイプは、複合[キー](#dictionary-key-and-fields)と共に使用されます。`cache`に似ています。

### ssd_cache

`s_cache`に似ていますが、データをSSDに保存しインデックスをRAMに保存します。更新キューに関連するすべてのキャッシュディクショナリ設定もSSDキャッシュディクショナリに適用できます。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。

``` xml
<layout>
    <ssd_cache>
        <!-- elementary read blockのサイズ（バイト単位）。
             SSDのページサイズに等しくすることをお勧めします。 -->
        <block_size>4096</block_size>
        <!-- キャッシュファイルの最大サイズ（バイト単位）。 -->
        <file_size>16777216</file_size>
        <!-- SSDから要素を読み込むためのRAMバッファのサイズ（バイト単位）。 -->
        <read_buffer_size>131072</read_buffer_size>
        <!-- SSDにフラッシュする前に要素を集約するためのRAMバッファのサイズ（バイト単位）。 -->
        <write_buffer_size>1048576</write_buffer_size>
        <!-- キャッシュファイルが保存されるパス。 -->
        <path>/var/lib/clickhouse/user_files/test_dict</path>
    </ssd_cache>
</layout>
```

または

``` sql
LAYOUT(SSD_CACHE(BLOCK_SIZE 4096 FILE_SIZE 16777216 READ_BUFFER_SIZE 1048576
    PATH '/var/lib/clickhouse/user_files/test_dict'))
```

### complex_key_ssd_cache

このストレージタイプは、複合[キー](#dictionary-key-and-fields)と共に使用されます。`ssd_cache`に似ています。

### direct

ディクショナリはメモリに保存されず、リクエストの処理中にソース に直接アクセスします。

ディクショナリキーは[UInt64](../../sql-reference/data-types/int-uint.md)型を持ちます。

ローカルファイルを除くすべての[ソース](#dictionary-sources)がサポートされます。

設定例:

``` xml
<layout>
  <direct />
</layout>
```

または

``` sql
LAYOUT(DIRECT())
```

### complex_key_direct

このストレージタイプは、複合[キー](#dictionary-key-and-fields)と共に使用されます。`direct`に似ています。

### ip_trie

このストレージタイプはネットワークプレフィックス（IPアドレス）をASNなどのメタデータにマッピングするためのものです。

**例**

ClickHouseにIPプレフィックスとマッピングが含まれるテーブルがあるとします:

```sql
CREATE TABLE my_ip_addresses (
	prefix String,
	asn UInt32,
	cca2 String
)
ENGINE = MergeTree
PRIMARY KEY prefix;
```

```sql
INSERT INTO my_ip_addresses VALUES
	('202.79.32.0/20', 17501, 'NP'),
    ('2620:0:870::/48', 3856, 'US'),
    ('2a02:6b8:1::/48', 13238, 'RU'),
    ('2001:db8::/32', 65536, 'ZZ')
;
```

このテーブルの`ip_trie`ディクショナリを定義しましょう。`ip_trie`レイアウトは複合キーを必要とします:

``` xml
<structure>
    <key>
        <attribute>
            <name>prefix</name>
            <type>String</type>
        </attribute>
    </key>
    <attribute>
            <name>asn</name>
            <type>UInt32</type>
            <null_value />
    </attribute>
    <attribute>
            <name>cca2</name>
            <type>String</type>
            <null_value>??</null_value>
    </attribute>
    ...
</structure>
<layout>
    <ip_trie>
        <!-- キー属性`prefix`はdictGetStringで取得できます。 -->
        <!-- このオプションはメモリ使用量を増やします。 -->
        <access_to_key_from_attributes>true</access_to_key_from_attributes>
    </ip_trie>
</layout>
```

または

``` sql
CREATE DICTIONARY my_ip_trie_dictionary (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'my_ip_addresses'))
LAYOUT(IP_TRIE)
LIFETIME(3600);
```

キーは許可されたIPプレフィックスを含む`String`型属性を一つだけ持たなければなりません。他の型はまだサポートされていません。

構文は以下の通りです:

``` sql
dictGetT('dict_name', 'attr_name', ip)
```

関数は`UInt32`（IPv4の場合）、または`FixedString(16)`（IPv6の場合）を取ります。例 :

``` sql
SELECT dictGet('my_ip_trie_dictionary', 'cca2', toIPv4('202.79.32.10')) AS result;

┌─result─┐
│ NP     │
└────────┘


SELECT dictGet('my_ip_trie_dictionary', 'asn', IPv6StringToNum('2001:db8::1')) AS result;

┌─result─┐
│  65536 │
└────────┘


SELECT dictGet('my_ip_trie_dictionary', ('asn', 'cca2'), IPv6StringToNum('2001:db8::1')) AS result;

┌─result───────┐
│ (65536,'ZZ') │
└──────────────┘
```

他の型はまだサポートされていません。関数は、このIPアドレスに対応するプレフィックスの属性を返します。プレフィックスが重複する場合、最も特定のものが返されます。

データは完全にRAMに収まる必要があります。

## LIFETIMEを使用したディクショナリデータの更新

ClickHouseは、`LIFETIME`タグ（秒単位で定義）に基づいて定期的にディクショナリを更新します。`LIFETIME`は完全にダウンロードされたディクショナリの更新間隔であり、キャッシュされたディクショナリの無効化間隔です。
```
更新の間、古いバージョンのDictionaryは依然としてクエリ可能です。Dictionaryの更新（初めてDictionaryをロードする場合を除く）はクエリをブロックしません。更新中にエラーが発生した場合、エラーはサーバーログに記録され、クエリは古いバージョンのDictionaryを使用して続行できます。Dictionaryの更新が成功した場合、古いバージョンのDictionaryはアトミックに置き換えられます。

設定の例:

<CloudDetails />

``` xml
<dictionary>
    ...
    <lifetime>300</lifetime>
    ...
</dictionary>
```

または

``` sql
CREATE DICTIONARY (...)
...
LIFETIME(300)
...
```

`<lifetime>0</lifetime>` (`LIFETIME(0)`) の設定は、Dictionaryの更新を防ぎます。

更新のための時間間隔を設定することができ、ClickHouseはこの範囲内で均一にランダムな時間を選びます。これは、多数のサーバーで更新する際に、Dictionaryソースへの負荷を分散するために必要です。

設定の例:

``` xml
<dictionary>
    ...
    <lifetime>
        <min>300</min>
        <max>360</max>
    </lifetime>
    ...
</dictionary>
```

または

``` sql
LIFETIME(MIN 300 MAX 360)
```

もし`<min>0</min>`および`<max>0</max>`の場合、ClickHouseはタイムアウトによってDictionaryをリロードしません。
この場合、Dictionaryの構成ファイルが変更された場合や`SYSTEM RELOAD DICTIONARY`コマンドが実行された場合には、ClickHouseはDictionaryを早くリロードすることがあります。

Dictionaryを更新する際、ClickHouseサーバーは[ソース](#dictionary-sources)のタイプに応じて異なるロジックを適用します。

- テキストファイルの場合、変更された時間を確認します。以前に記録された時間と異なる場合は、Dictionaryが更新されます。
- MySQLソースの場合、`SHOW TABLE STATUS`クエリを使用して変更された時間が確認されます（MySQL 8の場合、MySQLでメタ情報キャッシュを無効にする必要があります: `set global information_schema_stats_expiry=0`）。
- 他のソースからのDictionaryはデフォルトで毎回更新されます。

他のソース（ODBC、PostgreSQL、ClickHouseなど）の場合、実際に変更された場合にのみDictionaryを更新するクエリを設定できます。これを行うには、次の手順を実行します:

- Dictionaryテーブルには、ソースデータが更新されると常に変更されるフィールドが必要です。
- ソースの設定には、変更されるフィールドを取得するクエリを指定する必要があります。ClickHouseサーバーはクエリの結果を行として解釈し、この行が以前の状態と異なる場合にDictionaryを更新します。クエリは[ソース](#dictionary-sources)の設定の`<invalidate_query>`フィールドに指定します。

設定の例:

``` xml
<dictionary>
    ...
    <odbc>
      ...
      <invalidate_query>SELECT update_time FROM dictionary_source where id = 1</invalidate_query>
    </odbc>
    ...
</dictionary>
```

または

``` sql
...
SOURCE(ODBC(... invalidate_query 'SELECT update_time FROM dictionary_source where id = 1'))
...
```

`Cache`、`ComplexKeyCache`、`SSDCache`、および `SSDComplexKeyCache` Dictionaryは同期および非同期更新の両方をサポートしています。

また、`Flat`、`Hashed`、`ComplexKeyHashed` Dictionaryでは、前回の更新後に変更されたデータのみをリクエストすることが可能です。`update_field`がDictionaryソースの設定の一部として指定されている場合、前回の更新時刻（秒単位）の値がデータリクエストに追加されます。ソースタイプ（Executable, HTTP, MySQL, PostgreSQL, ClickHouse, または ODBC）に応じて、更新前に`update_field`に異なるロジックが適用されます。

- ソースがHTTPの場合、`update_field`はクエリパラメータとして最後の更新時刻をパラメータ値として追加されます。
- ソースがExecutableの場合、`update_field`は実行可能スクリプトの引数として最後の更新時刻を引数値として追加されます。
- ソースがClickHouse、MySQL、PostgreSQL、ODBCの場合、`update_field`はSQLクエリの最高レベルでの`WHERE`条件として、最後の更新時刻と大なりまたは等しいもので比較されます。デフォルトでは、この`WHERE`条件はSQLクエリの最上位でチェックされます。代替として、クエリ内の任意の他の`WHERE`句で`{condition}`キーワードを使用して条件をチェックできます。例:
    ``` sql
    ...
    SOURCE(CLICKHOUSE(...
        update_field 'added_time'
        QUERY '
            SELECT my_arr.1 AS x, my_arr.2 AS y, creation_time
            FROM (
                SELECT arrayZip(x_arr, y_arr) AS my_arr, creation_time
                FROM dictionary_source
                WHERE {condition}
            )'
    ))
    ...
    ```

`update_field`オプションが設定されている場合、追加のオプション`update_lag`を設定できます。`update_lag`オプションの値は前回の更新時刻から差し引かれた後に更新されたデータがリクエストされます。

設定の例:

``` xml
<dictionary>
    ...
        <clickhouse>
            ...
            <update_field>added_time</update_field>
            <update_lag>15</update_lag>
        </clickhouse>
    ...
</dictionary>
```

または

``` sql
...
SOURCE(CLICKHOUSE(... update_field 'added_time' update_lag 15))
...
```

## Dictionary Sources

<CloudDetails />

DictionaryはさまざまなソースからClickHouseに接続できます。

Dictionaryがxmlファイルを使用して構成されている場合、構成は次のようになります:

``` xml
<clickhouse>
  <dictionary>
    ...
    <source>
      <source_type>
        <!-- ソースの設定 -->
      </source_type>
    </source>
    ...
  </dictionary>
  ...
</clickhouse>
```

[DDL-query](../../sql-reference/statements/create/dictionary.md)の場合、上記の構成は次のようになります:

``` sql
CREATE DICTIONARY dict_name (...)
...
SOURCE(SOURCE_TYPE(param1 val1 ... paramN valN)) -- ソースの設定
...
```

ソースは`source`セクションで構成されます。

ソースタイプ[ローカルファイル](#local-file)、[実行ファイル](#executable-file)、[HTTP(S)](#https)、[ClickHouse](#clickhouse)のためのオプション設定が利用可能です:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
  <settings>
      <format_csv_allow_single_quotes>0</format_csv_allow_single_quotes>
  </settings>
</source>
```

または

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
SETTINGS(format_csv_allow_single_quotes = 0)
```

ソースの種類 (`source_type`):

- [ローカルファイル](#local-file)
- [実行ファイル](#executable-file)
- [実行プール](#executable-pool)
- [HTTP(S)](#https)
- DBMS
    - [ODBC](#odbc)
    - [MySQL](#mysql)
    - [ClickHouse](#clickhouse)
    - [MongoDB](#mongodb)
    - [Redis](#redis)
    - [Cassandra](#cassandra)
    - [PostgreSQL](#postgresql)

### Local File

設定の例:

``` xml
<source>
  <file>
    <path>/opt/dictionaries/os.tsv</path>
    <format>TabSeparated</format>
  </file>
</source>
```

または

``` sql
SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
```

設定フィールド:

- `path` – ファイルへの絶対パス。
- `format` – ファイルの形式。[Formats](../../interfaces/formats.md#formats)で説明されているすべての形式がサポートされています。

DDLコマンド（`CREATE DICTIONARY ...`）でソース`FILE`を持つDictionaryを作成する際には、ソースファイルを`user_files`ディレクトリに配置して、ClickHouseノード上の任意のファイルにデータベースユーザーがアクセスするのを防ぐ必要があります。

**関連項目**

- [Dictionary関数](../../sql-reference/table-functions/dictionary.md#dictionary-function)

### Executable File

実行ファイルの処理は、[Dictionaryがメモリ内にどのように保存されているか](#storing-dictionaries-in-memory)に依存します。Dictionaryが`cache`や`complex_key_cache`を使用して保存されている場合、ClickHouseはSTDINにリクエストを送信して必要なキーをリクエストします。それ以外の場合、ClickHouseは実行ファイルを開始し、その出力をDictionaryデータとして扱います。

設定の例:

``` xml
<source>
    <executable>
        <command>cat /opt/dictionaries/os.tsv</command>
        <format>TabSeparated</format>
        <implicit_key>false</implicit_key>
    </executable>
</source>
```

設定フィールド:

- `command` — 実行可能ファイルへの絶対パス、またはコマンドディレクトリが`PATH`にある場合はファイル名。
- `format` — ファイルの形式。[Formats](../../interfaces/formats.md#formats)で説明されているすべての形式がサポートされています。
- `command_termination_timeout` — 実行可能スクリプトはメインの読書-書き込みループを含むべきです。Dictionaryが破棄された後、パイプが閉じられ、ClickHouseが子プロセスにSIGTERMシグナルを送信する前に、実行可能ファイルは`command_termination_timeout`秒でシャットダウンさせられます。デフォルト値は10です。オプションのパラメータ。
- `command_read_timeout` - コマンドのstdoutからデータを読み取るタイムアウトをミリ秒単位で指定します。デフォルト値は10000です。オプションのパラメータ。
- `command_write_timeout` - コマンドのstdinにデータを書き込むタイムアウトをミリ秒単位で指定します。デフォルト値は10000です。オプションのパラメータ。
- `implicit_key` — 実行可能ソースファイルは値のみを返すことができ、要求されたキーとの対応は、結果内の行の順序によって暗黙的に決定されます。デフォルト値はfalseです。
- `execute_direct` - `execute_direct` = `1`の場合、`command`は[user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path)で指定されたuser_scriptsフォルダ内で検索されます。追加のスクリプト引数は空白区切りで指定できます。例: `script_name arg1 arg2`。`execute_direct` = `0`の場合、`command`は`bin/sh -c`の引数として渡されます。デフォルト値は`0`です。オプションのパラメータ。
- `send_chunk_header` - 処理するデータチャンクへの行数を送信するかどうかを制御します。オプション。デフォルト値は`false`です。

このDictionaryソースはXML構成でのみ設定できます。DDLを介して実行可能ソースを持つDictionaryを作成することはできません。さもないと、データベースユーザーはClickHouseノードで任意のバイナリを実行することができてしまいます。

### Executable Pool

実行プールは多くのプロセスプールからデータをロードすることを許可します。このソースは、ソースからすべてのデータをロードする必要があるDictionaryレイアウトでは動作しません。実行プールは、Dictionaryが`cache`、`complex_key_cache`、`ssd_cache`、`complex_key_ssd_cache`、`direct`、または`complex_key_direct`レイアウトを使用して保存されている場合に動作します。

実行プールは、指定されたコマンドでプロセスのプールを開始し、それらが終了するまでそれらを保持します。プログラムは、利用可能な限りSTDINからデータを読み取り、結果をSTDOUTに出力する必要があります。次のデータブロックをSTDINで待つことができます。ClickHouseはデータの処理後にSTDINを閉じませんが、必要に応じて新しいデータチャンクをパイプします。実行可能スクリプトは、この方法でのデータ処理に対応できるように準備する必要があります - STDINをポーリングし、データを早期にSTDOUTにフラッシュする必要があります。

設定の例:

``` xml
<source>
    <executable_pool>
        <command>while read key; do printf "$key\tData for key $key\n"; done</command>
        <format>TabSeparated</format>
        <pool_size>10</pool_size>
        <max_command_execution_time>10</max_command_execution_time>
        <implicit_key>false</implicit_key>
    </executable_pool>
</source>
```

設定フィールド:

- `command` — 実行可能ファイルへの絶対パス、またはプログラムディレクトリが`PATH`に書かれている場合はファイル名。
- `format` — ファイルの形式。[Formats](../../interfaces/formats.md#formats)で説明されているすべての形式がサポートされています。
- `pool_size` — プールのサイズ。`pool_size`として0が指定された場合、プールサイズの制限はありません。デフォルト値は`16`です。
- `command_termination_timeout` — 実行可能スクリプトはメイン読取書込ループを含む必要があります。Dictionaryが破棄された後、パイプは閉じられ、実行可能ファイルはClickHouseが子プロセスへSIGTERMシグナルを送信する前に`command_termination_timeout`秒でシャットダウンすることになります。秒単位で指定します。デフォルト値は10です。オプションのパラメータ。
- `max_command_execution_time` — データブロックを処理するための最大実行可能スクリプトコマンド実行時間。秒単位で指定します。デフォルト値は10です。オプションのパラメータ。
- `command_read_timeout` - コマンドstdoutからデータを読み取るタイムアウトをミリ秒単位で指定します。デフォルト値は10000です。オプションのパラメータ。
- `command_write_timeout` - コマンドstdinにデータを書き込むタイムアウトをミリ秒単位で指定します。デフォルト値は10000です。オプションのパラメータ。
- `implicit_key` — 実行可能ソースファイルは値のみを返すことができ、要求されたキーとの対応は、結果内の行の順序によって暗黙的に決定されます。デフォルト値はfalseです。オプションのパラメータ。
- `execute_direct` - `execute_direct` = `1`の場合、`command`は[user_scripts_path](../../operations/server-configuration-parameters/settings.md#user_scripts_path)で指定されたuser_scriptsフォルダ内で検索されます。追加のスクリプト引数は空白区切りで指定できます。例: `script_name arg1 arg2`。`execute_direct` = `0`の場合、`command`は`bin/sh -c`の引数として渡されます。デフォルト値は`1`です。オプションのパラメータ。
- `send_chunk_header` - どのようにして行数を処理するデータチャンクに送信するかを制御します。オプション。デフォルト値は`false`です。

このDictionaryソースはXML構成でのみ設定できます。DDLを介して実行可能ソースを持つDictionaryを作成することはできません。さもないと、データベースユーザーはClickHouseノードで任意のバイナリを実行することができてしまいます。

### HTTP(S)

HTTP(S)サーバーとの作業は、[Dictionaryがメモリーにどのように保存されているか](#storing-dictionaries-in-memory)に依存します。Dictionaryが`cache`や`complex_key_cache`を使用して保存されている場合、ClickHouseは`POST`メソッドを通じて必要なキーを要求します。

設定の例:

``` xml
<source>
    <http>
        <url>http://[::1]/os.tsv</url>
        <format>TabSeparated</format>
        <credentials>
            <user>user</user>
            <password>password</password>
        </credentials>
        <headers>
            <header>
                <name>API-KEY</name>
                <value>key</value>
            </header>
        </headers>
    </http>
</source>
```

または

``` sql
SOURCE(HTTP(
    url 'http://[::1]/os.tsv'
    format 'TabSeparated'
    credentials(user 'user' password 'password')
    headers(header(name 'API-KEY' value 'key'))
))
```

ClickHouseがHTTPSリソースにアクセスするためには、[openSSL](../../operations/server-configuration-parameters/settings.md#openssl)をサーバー構成に設定する必要があります。

設定フィールド:

- `url` – ソースURL。
- `format` – ファイルの形式。[Formats](../../interfaces/formats.md#formats)で説明されているすべての形式がサポートされています。
- `credentials` – 基本的なHTTP認証。オプションのパラメータ。
- `user` – 認証に必要なユーザー名。
- `password` – 認証に必要なパスワード。
- `headers` – HTTPリクエストに使用するすべてのカスタムHTTPヘッダエントリ。オプションのパラメータ。
- `header` – 単一のHTTPヘッダエントリ。
- `name` – リクエスト送信時に使用する識別子名。
- `value` – 特定の識別子名に設定された値。

DDLコマンド（`CREATE DICTIONARY ...`）を使用してDictionaryを作成すると、HTTP Dictionaryのリモートホストは任意のHTTPサーバーにアクセスしないように`remote_url_allow_hosts`セクションの内容に対してチェックされます。

### DBMS

#### ODBC

ODBCドライバがある任意のデータベースに接続できます。

設定の例:

``` xml
<source>
    <odbc>
        <db>DatabaseName</db>
        <table>ShemaName.TableName</table>
        <connection_string>DSN=some_parameters</connection_string>
        <invalidate_query>SQL_QUERY</invalidate_query>
        <query>SELECT id, value_1, value_2 FROM ShemaName.TableName</query>
    </odbc>
</source>
```

または

``` sql
SOURCE(ODBC(
    db 'DatabaseName'
    table 'SchemaName.TableName'
    connection_string 'DSN=some_parameters'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

設定フィールド:

- `db` – データベースの名前。接続文字列内にデータベース名が設定されている場合は省略可能。
- `table` – テーブルの名前とスキーマ。
- `connection_string` – 接続文字列。
- `invalidate_query` – Dictionaryの状態を確認するためのクエリ。オプションのパラメータ。[Refresh dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime)セクションを参照してください。
- `query` – カスタムクエリ。オプションのパラメータ。

:::note
`table`と`query`フィールドは一緒に使用できません。そして、`table`または`query`フィールドのどちらか一方を指定する必要があります。
:::

ClickHouseはODBCドライバからクォート記号を受け取り、ドライバへのクエリ内で設定をすべてクォートしますので、テーブル名はデータベース内のテーブル名のケースに応じて設定する必要があります。

Oracle使用時にはエンコーディングに問題がある場合は、該当する[FAQ](/knowledgebase/oracle-odbc)を参照してください。

##### ODBCDictionary機能の既知の脆弱性

:::note
ODBCドライバの`Servername`接続パラメータを通じてデータベースに接続する際に置き換えが生じる可能性があります。この場合、`odbc.ini`からの`USERNAME`と`PASSWORD`の値がリモートサーバーに送信されるため、これらが漏洩する可能性があります。
:::

**安全でない使用の例**

PostgreSQL用にunixODBCを設定します。`/etc/odbc.ini`の内容:

``` text
[gregtest]
Driver = /usr/lib/psqlodbca.so
Servername = localhost
PORT = 5432
DATABASE = test_db
#OPTION = 3
USERNAME = test
PASSWORD = test
```

この後、次のようなクエリを実行すると

``` sql
SELECT * FROM odbc('DSN=gregtest;Servername=some-server.com', 'test_db');
```

ODBCドライバは`odbc.ini`の`USERNAME`と`PASSWORD`の値を`some-server.com`に送信します。

##### PostgreSQLへの接続例

Ubuntu OS。

unixODBCとPostgreSQL用ODBCドライバをインストールします:

``` bash
$ sudo apt-get install -y unixodbc odbcinst odbc-postgresql
```

`/etc/odbc.ini`（またはClickHouseを実行しているユーザーでサインインしている場合は`~/.odbc.ini`）を設定します:

``` text
    [DEFAULT]
    Driver = myconnection

    [myconnection]
    Description         = PostgreSQL connection to my_db
    Driver              = PostgreSQL Unicode
    Database            = my_db
    Servername          = 127.0.0.1
    UserName            = username
    Password            = password
    Port                = 5432
    Protocol            = 9.3
    ReadOnly            = No
    RowVersioning       = No
    ShowSystemTables    = No
    ConnSettings        =
```

ClickHouseのDictionary設定:

``` xml
<clickhouse>
    <dictionary>
        <name>table_name</name>
        <source>
            <odbc>
                <!-- 以下のパラメータをconnection_stringで指定できます: -->
                <!-- DSN=myconnection;UID=username;PWD=password;HOST=127.0.0.1;PORT=5432;DATABASE=my_db -->
                <connection_string>DSN=myconnection</connection_string>
                <table>postgresql_table</table>
            </odbc>
        </source>
        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>
        <layout>
            <hashed/>
        </layout>
        <structure>
            <id>
                <name>id</name>
            </id>
            <attribute>
                <name>some_column</name>
                <type>UInt64</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

または

``` sql
CREATE DICTIONARY table_name (
    id UInt64,
    some_column UInt64 DEFAULT 0
)
PRIMARY KEY id
SOURCE(ODBC(connection_string 'DSN=myconnection' table 'postgresql_table'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 360)
```

ドライバがあるライブラリへのフルパスを指定するために`odbc.ini`を編集する必要があるかもしれません：`DRIVER=/usr/local/lib/psqlodbcw.so`。

##### MS SQL Serverへの接続例

Ubuntu OS。

MS SQLに接続するためのODBCドライバのインストール:

``` bash
$ sudo apt-get install tdsodbc freetds-bin sqsh
```

ドライバの設定:

```bash
    $ cat /etc/freetds/freetds.conf
    ...

    [MSSQL]
    host = 192.168.56.101
    port = 1433
    tds version = 7.0
    client charset = UTF-8

    # TDS接続のテスト
    $ sqsh -S MSSQL -D database -U user -P password


    $ cat /etc/odbcinst.ini

    [FreeTDS]
    Description     = FreeTDS
    Driver          = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
    Setup           = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
    FileUsage       = 1
    UsageCount      = 5

    $ cat /etc/odbc.ini
    # $ cat ~/.odbc.ini # ClickHouseを実行しているユーザーでサインインしている場合

    [MSSQL]
    Description     = FreeTDS
    Driver          = FreeTDS
    Servername      = MSSQL
    Database        = test
    UID             = test
    PWD             = test
    Port            = 1433


    # （オプション）ODBC接続のテスト（isqlツールを使用するには[unixodbc](https://packages.debian.org/sid/unixodbc)パッケージをインストールします）
    $ isql -v MSSQL "user" "password"
```

備考:
- 特定のSQL Serverバージョンでサポートされる最も早いTDSバージョンを確認するには、製品のドキュメントを参照するか、[MS-TDS製品動作](https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/135d0ebe-5c4c-4a94-99bf-1811eccb9f4a)を見てください。

ClickHouseでのDictionaryの設定:

``` xml
<clickhouse>
    <dictionary>
        <name>test</name>
        <source>
            <odbc>
                <table>dict</table>
                <connection_string>DSN=MSSQL;UID=test;PWD=test</connection_string>
            </odbc>
        </source>

        <lifetime>
            <min>300</min>
            <max>360</max>
        </lifetime>

        <layout>
            <flat />
        </layout>

        <structure>
            <id>
                <name>k</name>
            </id>
            <attribute>
                <name>s</name>
                <type>String</type>
                <null_value></null_value>
            </attribute>
        </structure>
    </dictionary>
</clickhouse>
```

または

``` sql
CREATE DICTIONARY test (
    k UInt64,
    s String DEFAULT ''
)
PRIMARY KEY k
SOURCE(ODBC(table 'dict' connection_string 'DSN=MSSQL;UID=test;PWD=test'))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 360)
```

#### MySQL

設定の例:

``` xml
<source>
  <mysql>
      <port>3306</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <replica>
          <host>example01-1</host>
          <priority>1</priority>
      </replica>
      <replica>
          <host>example01-2</host>
          <priority>1</priority>
      </replica>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

または

``` sql
SOURCE(MYSQL(
    port 3306
    user 'clickhouse'
    password 'qwerty'
    replica(host 'example01-1' priority 1)
    replica(host 'example01-2' priority 1)
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

設定フィールド:

- `port` – MySQLサーバーのポート。すべてのレプリカに対して、または個別に（`<replica>`内）指定できます。

- `user` – MySQLユーザー名。すべてのレプリカに対して、または個別に（`<replica>`内）指定できます。

- `password` – MySQLユーザーのパスワード。すべてのレプリカに対して、または個別に（`<replica>`内）指定できます。

- `replica` – レプリカ構成のセクション。複数のセクションを作成できます。

        - `replica/host` – MySQLホスト。
        - `replica/priority` – レプリカの優先度。ClickHouseが接続を試みる際は、優先度に基づいてレプリカを巡回します。数が少ないほど優先度が高くなります。

- `db` – データベースの名前。

- `table` – テーブルの名前。

- `where` – 選択条件。条件の構文はMySQLの`WHERE`句と同じで、たとえば`id > 10 AND id < 20`のようになります。オプションのパラメータ。

- `invalidate_query` – Dictionaryの状態を確認するためのクエリ。オプションのパラメータ。[Refresh dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime)セクションを参照してください。

- `fail_on_connection_loss` – 接続喪失時のサーバーの動作を制御する構成パラメータ。`true`の場合、クライアントとサーバー間の接続が失われた場合に即座に例外がスローされます。`false`の場合、例外がスローされる前にClickHouseサーバーはクエリを3回再試行します。再試行により応答時間が増加することに注意してください。デフォルト値: `false`。

- `query` – カスタムクエリ。オプションのパラメータ。

:::note
`table`または`where`フィールドは、`query`フィールドと一緒に使用することはできません。そして、`table`または`query`フィールドのどちらか一方を指定する必要があります。
:::

:::note
明示的なパラメータ`secure`はありません。SSL接続を確立する際は、セキュリティが必須です。
:::

MySQLはローカルホスト上でソケットを介して接続できます。この場合、`host`と`socket`を設定します。

設定の例:

``` xml
<source>
  <mysql>
      <host>localhost</host>
      <socket>/path/to/socket/file.sock</socket>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <fail_on_connection_loss>true</fail_on_connection_loss>
	  <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </mysql>
</source>
```

または

``` sql
SOURCE(MYSQL(
    host 'localhost'
    socket '/path/to/socket/file.sock'
    user 'clickhouse'
    password 'qwerty'
    db 'db_name'
    table 'table_name'
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    fail_on_connection_loss 'true'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

#### ClickHouse

設定の例:

``` xml
<source>
    <clickhouse>
        <host>example01-01-1</host>
        <port>9000</port>
        <user>default</user>
        <password></password>
        <db>default</db>
        <table>ids</table>
        <where>id=10</where>
        <secure>1</secure>
		<query>SELECT id, value_1, value_2 FROM default.ids</query>
    </clickhouse>
</source>
```

または

``` sql
SOURCE(CLICKHOUSE(
    host 'example01-01-1'
    port 9000
    user 'default'
    password ''
    db 'default'
    table 'ids'
    where 'id=10'
    secure 1
	query 'SELECT id, value_1, value_2 FROM default.ids'
));
```

設定フィールド:

- `host` – ClickHouseホスト。ローカルホストの場合、クエリはネットワークを使用せずに処理されます。フォールトトレランスを向上させるために、[分散テーブル](../../engines/table-engines/special/distributed.md)を作成し、それを後続の設定に入力することができます。
- `port` – ClickHouseサーバーのポート。
- `user` – ClickHouseユーザーの名前。
- `password` – ClickHouseユーザーのパスワード。
- `db` – データベースの名前。
- `table` – テーブルの名前。
- `where` – 選択条件。省略可能です。
- `invalidate_query` – Dictionaryの状態を確認するためのクエリ。オプションのパラメータ。[Refresh dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime)セクションを参照してください。
- `secure` - 接続にSSLを使用。
- `query` – カスタムクエリ。オプションのパラメータ。

:::note
`table`または`where`フィールドは、`query`フィールドと一緒に使用することはできません。そして、`table`または`query`フィールドのどちらか一方を指定する必要があります。
:::

#### MongoDB

設定の例:

``` xml
<source>
    <mongodb>
        <host>localhost</host>
        <port>27017</port>
        <user></user>
        <password></password>
        <db>test</db>
        <collection>dictionary_source</collection>
        <options>ssl=true</options>
    </mongodb>
</source>
```

または

``` xml
<source>
    <mongodb>
        <uri>mongodb://localhost:27017/test?ssl=true</uri>
        <collection>dictionary_source</collection>
    </mongodb>
</source>
```

または

``` sql
SOURCE(MONGODB(
    host 'localhost'
    port 27017
    user ''
    password ''
    db 'test'
    collection 'dictionary_source'
    options 'ssl=true'
))
```

設定フィールド:

- `host` – MongoDBホスト。
- `port` – MongoDBサーバーのポート。
- `user` – MongoDBユーザー名。
- `password` – MongoDBユーザーのパスワード。
- `db` – データベースの名前。
- `collection` – コレクションの名前。
- `options` -  MongoDB接続文字列オプション（オプションのパラメータ）。

または

``` sql
SOURCE(MONGODB(
    uri 'mongodb://localhost:27017/clickhouse'
    collection 'dictionary_source'
))
```

設定フィールド:

- `uri` - 接続を確立するためのURI。
- `collection` – コレクションの名前。

[エンジンの詳細情報](../../engines/table-engines/integrations/mongodb.md)


#### Redis

設定の例:

``` xml
<source>
    <redis>
        <host>localhost</host>
        <port>6379</port>
        <storage_type>simple</storage_type>
        <db_index>0</db_index>
    </redis>
</source>
```

または

``` sql
SOURCE(REDIS(
    host 'localhost'
    port 6379
    storage_type 'simple'
    db_index 0
))
```

設定フィールド:

- `host` – Redisホスト。
- `port` – Redisサーバーのポート。
- `storage_type` – キーを操作するために使用される内部のRedisストレージ構造。`simple`は単純なソースおよび単一のキーソースにハッシュされたものに、`hash_map`は2つのキーを持つハッシュされたソースに使用されます。範囲指定されたソースや、複雑なキーを持つキャッシュソースはサポートされていません。省略可能、デフォルト値は`simple`です。
- `db_index` – 特定の数値インデックスのRedis論理データベース。省略可能、デフォルト値は0です。

#### Cassandra

設定の例:

``` xml
<source>
    <cassandra>
        <host>localhost</host>
        <port>9042</port>
        <user>username</user>
        <password>qwerty123</password>
        <keyspase>database_name</keyspase>
        <column_family>table_name</column_family>
        <allow_filtering>1</allow_filtering>
```
```xml
<partition_key_prefix>1</partition_key_prefix>
<consistency>One</consistency>
<where>"SomeColumn" = 42</where>
<max_threads>8</max_threads>
<query>SELECT id, value_1, value_2 FROM database_name.table_name</query>
</cassandra>
</source>
```

設定項目:

- `host` – Cassandraホスト、またはカンマ区切りのホストリスト。
- `port` – Cassandraサーバーのポート。指定されていない場合はデフォルトポート9042が使用されます。
- `user` – Cassandraユーザーの名前。
- `password` – Cassandraユーザーのパスワード。
- `keyspace` – キースペース（データベース）の名前。
- `column_family` – カラムファミリー（テーブル）の名前。
- `allow_filtering` – クラスタリングキーカラムに対する潜在的に高コストな条件を許可するかどうかを示すフラグ。デフォルト値は1です。
- `partition_key_prefix` – Cassandraテーブルの主キー内のパーティションキーカラムの数。複合キーDictionaryに必要です。Dictionary定義のキーの順序はCassandraと同じでなければなりません。デフォルト値は1（最初のキーはパーティションキーで、他のキーはクラスタリングキー）です。
- `consistency` – 一貫性レベル。可能な値は:`One`, `Two`, `Three`, `All`, `EachQuorum`, `Quorum`, `LocalQuorum`, `LocalOne`, `Serial`, `LocalSerial`です。デフォルト値は`One`です。
- `where` – オプションの選択条件。
- `max_threads` – 複合キーDictionaryで複数のパーティションからデータをロードするために使用する最大スレッド数。
- `query` – カスタムクエリ。オプションのパラメータ。

:::note
`column_family`または`where`フィールドは`query`フィールドと一緒に使用できません。また、`column_family`または`query`のいずれか一方のフィールドを必ず宣言する必要があります。
:::

#### PostgreSQL

設定の例:

```xml
<source>
  <postgresql>
      <host>postgresql-hostname</host>
      <port>5432</port>
      <user>clickhouse</user>
      <password>qwerty</password>
      <db>db_name</db>
      <table>table_name</table>
      <where>id=10</where>
      <invalidate_query>SQL_QUERY</invalidate_query>
      <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
  </postgresql>
</source>
```

または

```sql
SOURCE(POSTGRESQL(
    port 5432
    host 'postgresql-hostname'
    user 'postgres_user'
    password 'postgres_password'
    db 'db_name'
    table 'table_name'
    replica(host 'example01-1' port 5432 priority 1)
    replica(host 'example01-2' port 5432 priority 2)
    where 'id=10'
    invalidate_query 'SQL_QUERY'
    query 'SELECT id, value_1, value_2 FROM db_name.table_name'
))
```

設定項目:

- `host` – PostgreSQLサーバーのホスト。すべてのレプリカに対して、またはそれぞれ個別に指定できます（`<replica>`内）。
- `port` – PostgreSQLサーバーのポート。すべてのレプリカに対して、またはそれぞれ個別に指定できます（`<replica>`内）。
- `user` – PostgreSQLユーザーの名前。すべてのレプリカに対して、またはそれぞれ個別に指定できます（`<replica>`内）。
- `password` – PostgreSQLユーザーのパスワード。すべてのレプリカに対して、またはそれぞれ個別に指定できます（`<replica>`内）。
- `replica` – レプリカ設定セクションが複数あります:
    - `replica/host` – PostgreSQLホスト。
    - `replica/port` – PostgreSQLポート。
    - `replica/priority` – レプリカの優先順位。接続を試みる際、ClickHouseは優先順位の順にレプリカを試みます。数字が小さいほど、優先順位が高くなります。
- `db` – データベースの名前。
- `table` – テーブルの名前。
- `where` – 選択基準。条件の構文はPostgreSQLの`WHERE`句と同じです。例えば`id > 10 AND id < 20`。オプションのパラメータ。
- `invalidate_query` – Dictionaryの状態を確認するためのクエリ。オプションのパラメータ。詳細は[Refreshing dictionary data using LIFETIME](#refreshing-dictionary-data-using-lifetime)セクションを参照してください。
- `query` – カスタムクエリ。オプションのパラメータ。

:::note
`table`または`where`フィールドは`query`フィールドと一緒に使用できません。また、`table`または`query`のいずれか一方のフィールドを必ず宣言する必要があります。
:::

### Null

ダミー（空）のDictionaryを作成するために使用できる特別なソースです。このようなDictionaryは、テストや分散テーブルのデータとクエリノードが分離されたセットアップで役立ちます。

```sql
CREATE DICTIONARY null_dict (
    id              UInt64,
    val             UInt8,
    default_val     UInt8 DEFAULT 123,
    nullable_val    Nullable(UInt8)
)
PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(0);
```

## Dictionary Key and Fields

<CloudDetails />

`structure`句はクエリ用のDictionaryキーとフィールドを記述します。

XMLの記述:

```xml
<dictionary>
    <structure>
        <id>
            <name>Id</name>
        </id>

        <attribute>
            <!-- 属性のパラメータ -->
        </attribute>

        ...

    </structure>
</dictionary>
```

属性は以下の要素で記述されます:

- `<id>` — キーカラム
- `<attribute>` — データカラム: 複数の属性を持つことができます。

DDLクエリ:

```sql
CREATE DICTIONARY dict_name (
    Id UInt64,
    -- attributes
)
PRIMARY KEY Id
...
```

属性はクエリ本文で記述されます:

- `PRIMARY KEY` — キーカラム
- `AttrName AttrType` — データカラム。複数の属性を持つことができます。

## Key

ClickHouseは以下のタイプのキーをサポートします:

- 数値キー。`UInt64`。`<id>`タグまたは`PRIMARY KEY`キーワードで定義されます。
- 複合キー。異なるタイプの値の集合。`<key>`タグまたは`PRIMARY KEY`キーワードで定義されます。

XML構造には`<id>`または`<key>`のいずれかを含めることができます。DDLクエリには`PRIMARY KEY`が1つ含まれている必要があります。

:::note
キーを属性として記述してはいけません。
:::

### Numeric Key

タイプ: `UInt64`。

設定例:

```xml
<id>
    <name>Id</name>
</id>
```

設定項目:

- `name` – キーを持つカラムの名前。

DDLクエリの例:

```sql
CREATE DICTIONARY (
    Id UInt64,
    ...
)
PRIMARY KEY Id
...
```

- `PRIMARY KEY` – キーを持つカラムの名前。

### Composite Key

キーはあらゆるタイプのフィールドの`tuple`にすることができます。この場合の[レイアウト](#storing-dictionaries-in-memory)は`complex_key_hashed`または`complex_key_cache`である必要があります。

:::tip
複合キーは単一要素で構成することもできます。これにより、例えば文字列をキーとして使用することが可能になります。
:::

キー構造は`<key>`要素で設定されます。キー・フィールドはDictionaryの[attributes](#dictionary-key-and-fields)と同じ形式で指定されます。例:

```xml
<structure>
    <key>
        <attribute>
            <name>field1</name>
            <type>String</type>
        </attribute>
        <attribute>
            <name>field2</name>
            <type>UInt32</type>
        </attribute>
        ...
    </key>
...
```

または

```sql
CREATE DICTIONARY (
    field1 String,
    field2 String
    ...
)
PRIMARY KEY field1, field2
...
```

`dictGet*`関数に対するクエリでは、キーとしてタプルが渡されます。例: `dictGetString('dict_name', 'attr_name', tuple('string for field1', num_for_field2))`。

## Attributes

設定例:

```xml
<structure>
    ...
    <attribute>
        <name>Name</name>
        <type>ClickHouseDataType</type>
        <null_value></null_value>
        <expression>rand64()</expression>
        <hierarchical>true</hierarchical>
        <injective>true</injective>
        <is_object_id>true</is_object_id>
    </attribute>
</structure>
```

または

```sql
CREATE DICTIONARY somename (
    Name ClickHouseDataType DEFAULT '' EXPRESSION rand64() HIERARCHICAL INJECTIVE IS_OBJECT_ID
)
```

設定項目:

| タグ | 説明 | 必須項目 |
|------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| `name` | カラム名。 | はい |
| `type` | ClickHouseデータタイプ: [UInt8](../../sql-reference/data-types/int-uint.md), [UInt16](../../sql-reference/data-types/int-uint.md), [UInt32](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md), [Int8](../../sql-reference/data-types/int-uint.md), [Int16](../../sql-reference/data-types/int-uint.md), [Int32](../../sql-reference/data-types/int-uint.md), [Int64](../../sql-reference/data-types/int-uint.md), [Float32](../../sql-reference/data-types/float.md), [Float64](../../sql-reference/data-types/float.md), [UUID](../../sql-reference/data-types/uuid.md), [Decimal32](../../sql-reference/data-types/decimal.md), [Decimal64](../../sql-reference/data-types/decimal.md), [Decimal128](../../sql-reference/data-types/decimal.md), [Decimal256](../../sql-reference/data-types/decimal.md),[Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md), [String](../../sql-reference/data-types/string.md), [Array](../../sql-reference/data-types/array.md)。<br/>ClickHouseはDictionaryから指定されたデータタイプに値をキャストしようとします。例えば、MySQLではフィールドは`TEXT`、`VARCHAR`、または`BLOB`であるかもしれませんが、ClickHouseでは`String`としてアップロードできます。<br/>[Nullable](../../sql-reference/data-types/nullable.md)は現在、[Flat](#flat), [Hashed](#hashed), [ComplexKeyHashed](#complex_key_hashed), [Direct](#direct), [ComplexKeyDirect](#complex_key_direct), [RangeHashed](#range_hashed), Polygon, [Cache](#cache), [ComplexKeyCache](#complex_key_cache), [SSDCache](#ssd_cache), [SSDComplexKeyCache](#complex_key_ssd_cache) Dictionaryでサポートされています。[IPTrie](#ip_trie) Dictionaryでは `Nullable` タイプはサポートされていません。 | はい |
| `null_value` | 存在しない要素のデフォルト値。<br/>例では空の文字列です。[NULL](../syntax.md#null) 値は`Nullable`タイプのみで使用可能です（上記のタイプの説明を参照）。 | はい |
| `expression` | ClickHouseが値に対して実行する[式](../../sql-reference/syntax.md#expressions)。<br/>式はリモートSQLデータベースのカラム名であることができます。したがって、リモートカラムのエイリアスを作成するために使用できます。<br/><br/>デフォルト値: 式なし。 | いいえ |
| <a name="hierarchical-dict-attr"></a> `hierarchical` | `true`の場合、属性は現在のキーの親キーの値を含みます。[階層Dictionary](#hierarchical-dictionaries)を参照してください。<br/><br/>デフォルト値: `false`。 | いいえ |
| `injective` | `id -> attribute`の写像が[単射](https://en.wikipedia.org/wiki/Injective_function)であるかどうかを示すフラグ。<br/>`true`の場合、ClickHouseは`GROUP BY`句の後に単射のDictionaryへの要求を自動的に配置できます。通常、これによりこのような要求の量が大幅に削減されます。<br/><br/>デフォルト値: `false`。 | いいえ |
| `is_object_id` | クエリが`ObjectID`によるMongoDBドキュメントの実行であるかどうかを示すフラグ。<br/><br/>デフォルト値: `false`。

## 階層Dictionary

ClickHouseは[数値キー](#numeric-key)を持つ階層Dictionaryをサポートします。

以下のような階層構造を考えてみましょう:

```text
0 (共通の親)
│
├── 1 (ロシア)
│   │
│   └── 2 (モスクワ)
│       │
│       └── 3 (中心)
│
└── 4 (イギリス)
    │
    └── 5 (ロンドン)
```

この階層は次のDictionaryテーブルで表現できます。

| region_id | parent_region | region_name  |
|-----------|---------------|--------------|
| 1         | 0             | ロシア        |
| 2         | 1             | モスクワ     |
| 3         | 2             | 中心          |
| 4         | 0             | イギリス     |
| 5         | 4             | ロンドン    |

このテーブルには、`parent_region`カラムがあり、それは要素の最も近い親のキーを含んでいます。

ClickHouseは外部Dictionary属性に対して階層プロパティをサポートしています。このプロパティを使用すると、上記のように階層Dictionaryを設定できます。

[dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy)関数を使用すると、要素の親チェーンを取得できます。

我々の例では、Dictionaryの構造は次のようになります:

```xml
<dictionary>
    <structure>
        <id>
            <name>region_id</name>
        </id>

        <attribute>
            <name>parent_region</name>
            <type>UInt64</type>
            <null_value>0</null_value>
            <hierarchical>true</hierarchical>
        </attribute>

        <attribute>
            <name>region_name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

    </structure>
</dictionary>
```

## ポリゴンDictionary {#polygon-dictionaries}

ポリゴンDictionaryは、指定された点を含むポリゴンの検索を効率的に行うことができます。
例えば、地理座標で都市地域を定義することができます。

ポリゴンDictionary設定の例:

<CloudDetails />

```xml
<dictionary>
    <structure>
        <key>
            <attribute>
                <name>key</name>
                <type>Array(Array(Array(Array(Float64))))</type>
            </attribute>
        </key>

        <attribute>
            <name>name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>

        <attribute>
            <name>value</name>
            <type>UInt64</type>
            <null_value>0</null_value>
        </attribute>
    </structure>

    <layout>
        <polygon>
            <store_polygon_key_column>1</store_polygon_key_column>
        </polygon>
    </layout>

    ...
</dictionary>
```

対応する[DDLクエリ](../../sql-reference/statements/create/dictionary.md#create-dictionary-query):

```sql
CREATE DICTIONARY polygon_dict_name (
    key Array(Array(Array(Array(Float64)))),
    name String,
    value UInt64
)
PRIMARY KEY key
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
...
```

ポリゴンDictionaryを設定する際、キーは以下のタイプのいずれかでなければなりません:

- 単純なポリゴン: 点の配列。
- マルチポリゴン: ポリゴンの配列。各ポリゴンは点の2次元配列です。この配列の最初の要素がポリゴンの外部境界であり、後続の要素はそれから除外される領域を指定します。

点は、それぞれの座標を表す配列またはタプルとして指定できます。現在の実装では、2次元点のみがサポートされています。

ユーザーは、ClickHouseがサポートするすべてのフォーマットで独自のデータをアップロードできます。

次の3種類の[インメモリストレージ](#storing-dictionaries-in-memory)が利用可能です:

- `POLYGON_SIMPLE`: クエリごとにすべてのポリゴンを線形に通過し、追加のインデックスを使用せずにそれぞれの所属を確認するナイーブな実装です。

- `POLYGON_INDEX_EACH`: 地理的地域向けに最適化されていて、ほとんどの場合すばやく所属が確認できるように、各ポリゴンに個別のインデックスが構築されます。
また、エリアにグリッドが重ねられ、候補となるポリゴンの数を大幅に絞り込みます。
グリッドは、セルを16等分に再帰的に分割することで作成され、2つのパラメータで設定されます。
分割は、再帰の深さが`MAX_DEPTH`に達するか、セルが横断するポリゴンの数が`MIN_INTERSECTIONS`に達すると終了します。
クエリに応答するには、対応するセルの中に保存されているポリゴンのインデックスにアクセスされます。

- `POLYGON_INDEX_CELL`: 上記のグリッドも作成されます。同じオプションが利用可能です。各シートセルに対して、その中に入るポリゴン片すべてに対してインデックスが構築されており、クエリに迅速に応答できます。

- `POLYGON`: `POLYGON_INDEX_CELL`の別名です。

Dictionaryクエリは、Dictionaryを操作するための標準的な[関数](../../sql-reference/functions/ext-dict-functions.md)を通じて行われます。
重要な違いは、ここではキーポイントが指定され、指定された点を含む最小のポリゴンが見つかる点です。

**例**

上で定義されたDictionaryと連動する例:

```sql
CREATE TABLE points (
    x Float64,
    y Float64
)
...
SELECT tuple(x, y) AS key, dictGet(dict_name, 'name', key), dictGet(dict_name, 'value', key) FROM points ORDER BY x, y;
```

最後のコマンドを実行した結果、`points`テーブル内の各ポイントに対して、それを含む最小領域ポリゴンが見つかり、要求された属性が出力されます。

**例**

ポリゴンDictionaryからSELECTクエリを介してカラムを読み取ることができます。Dictionary設定または対応するDDLクエリで`store_polygon_key_column = 1`をオンにしてください。

クエリ:

```sql
CREATE TABLE polygons_test_table
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
) ENGINE = TinyLog;

INSERT INTO polygons_test_table VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY polygons_test_dictionary
(
    key Array(Array(Array(Tuple(Float64, Float64)))),
    name String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(TABLE 'polygons_test_table'))
LAYOUT(POLYGON(STORE_POLYGON_KEY_COLUMN 1))
LIFETIME(0);

SELECT * FROM polygons_test_dictionary;
```

結果:

```text
┌─key─────────────────────────────┬─name──┐
│ [[[(3,1),(0,1),(0,-1),(3,-1)]]] │ Value │
└─────────────────────────────────┴───────┘
```

## 正規表現ツリーDictionary {#regexp-tree-dictionary}

正規表現ツリーDictionaryは、正規表現のツリーを使ってキーから属性へのマッピングを表現する特別なタイプのDictionaryです。例えば、[ユーザーエージェント](https://en.wikipedia.org/wiki/User_agent)文字列の解析のようなユースケースは、正規表現ツリーDictionaryで優雅に表現できます。

### ClickHouse Open-Sourceでの正規表現ツリーDictionaryの使用
正規表現ツリーディクショナリは、ClickHouseオープンソースで、正規表現ツリーを含むYAMLファイルのパスを提供する`YAMLRegExpTree`ソースを使用して定義されます。

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
...
```

ディクショナリソース`YAMLRegExpTree`は正規表現ツリーの構造を表します。例えば：

```yaml
- regexp: 'Linux/(\d+[\.\d]*).+tlinux'
  name: 'TencentOS'
  version: '\1'

- regexp: '\d+/tclwebkit(?:\d+[\.\d]*)'
  name: 'Android'
  versions:
    - regexp: '33/tclwebkit'
      version: '13'
    - regexp: '3[12]/tclwebkit'
      version: '12'
    - regexp: '30/tclwebkit'
      version: '11'
    - regexp: '29/tclwebkit'
      version: '10'
```

この設定は正規表現ツリーノードのリストで構成されています。各ノードは以下の構造を持ちます：

- **regexp**: ノードの正規表現。
- **attributes**: ユーザー定義ディクショナリアトリビュートのリスト。この例では、2つのアトリビュート`name`と`version`があります。最初のノードは両方のアトリビュートを定義しています。2番目のノードはアトリビュート`name`のみを定義しています。アトリビュート`version`は2番目のノードの子ノードから提供されます。
  - アトリビュートの値は、**バックリファレンス**を含むことがあり、正規表現のキャプチャグループを参照します。この例では、最初のノードのアトリビュート`version`の値は、正規表現のキャプチャグループ`(\d+[\.\d]*)`へのバックリファレンス`\1`を含んでいます。バックリファレンス番号は1から9の範囲で、`$1`または`\1`（番号1の場合）として書かれます。バックリファレンスはクエリ実行中のマッチしたキャプチャグループで置き換えられます。
- **child nodes**: 正規表現ツリーノードの子のリストで、それぞれが独自のアトリビュートと（可能な限り）子ノードを持ちます。文字列のマッチングは深さ優先で進行します。もし文字列が正規表現ノードと一致すると、ディクショナリはノードの子ノードとも一致するか確認します。その場合、最も深くマッチしたノードのアトリビュートが割り当てられます。子ノードのアトリビュートは親ノードの同名アトリビュートを上書きします。YAMLファイル内の子ノード名は任意で、上記の例では`versions`とされています。

正規表現ツリーディクショナリは`dictGet`、`dictGetOrDefault`、および`dictGetAll`関数を使用してアクセスのみ許可されます。

例：

```sql
SELECT dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024');
```

結果：

```text
┌─dictGet('regexp_dict', ('name', 'version'), '31/tclwebkit1024')─┐
│ ('Android','12')                                                │
└─────────────────────────────────────────────────────────────────┘
```

この場合、最初にトップレイヤーの2番目のノードの正規表現`\d+/tclwebkit(?:\d+[\.\d]*)`と一致します。ディクショナリはさらに子ノードを調べ、文字列が`3[12]/tclwebkit`と一致することも確認します。その結果、アトリビュート`name`の値は`Android`（最初のレイヤーで定義）で、アトリビュート`version`の値は`12`（子ノードで定義）となります。

強力なYAML設定ファイルを使用することで、正規表現ツリーディクショナリをユーザーエージェント文字列パーサーとして使用できます。私たちは[uap-core](https://github.com/ua-parser/uap-core)をサポートしており、機能テスト[02504_regexp_dictionary_ua_parser](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/02504_regexp_dictionary_ua_parser.sh)でその使用方法を示しています。

#### アトリビュート値の収集

時には、リーフノードの値だけでなく、マッチした複数の正規表現から値を返すことが有益です。このような場合には、特殊化された[`dictGetAll`](../../sql-reference/functions/ext-dict-functions.md#dictgetall)関数を使用できます。ノードに型`T`のアトリビュート値がある場合、`dictGetAll`は0またはそれ以上の値を含む`Array(T)`を返します。

デフォルトでは、キーごとに返されるマッチ数は制限されません。制限はオプションの第4引数として`dictGetAll`に渡すことができます。配列は_トポロジカル順_で埋められ、子ノードが親ノードの前に来て、兄弟ノードはソース内の順序に従います。

例：

```sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    tag String,
    topological_index Int64,
    captured Nullable(String),
    parent String
)
PRIMARY KEY(regexp)
SOURCE(YAMLRegExpTree(PATH '/var/lib/clickhouse/user_files/regexp_tree.yaml'))
LAYOUT(regexp_tree)
LIFETIME(0)
```

```yaml
# /var/lib/clickhouse/user_files/regexp_tree.yaml
- regexp: 'clickhouse\.com'
  tag: 'ClickHouse'
  topological_index: 1
  paths:
    - regexp: 'clickhouse\.com/docs(.*)'
      tag: 'ClickHouse Documentation'
      topological_index: 0
      captured: '\1'
      parent: 'ClickHouse'

- regexp: '/docs(/|$)'
  tag: 'Documentation'
  topological_index: 2

- regexp: 'github.com'
  tag: 'GitHub'
  topological_index: 3
  captured: 'NULL'
```

```sql
CREATE TABLE urls (url String) ENGINE=MergeTree ORDER BY url;
INSERT INTO urls VALUES ('clickhouse.com'), ('clickhouse.com/docs/ja'), ('github.com/clickhouse/tree/master/docs');
SELECT url, dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2) FROM urls;
```

結果：

```text
┌─url────────────────────────────────────┬─dictGetAll('regexp_dict', ('tag', 'topological_index', 'captured', 'parent'), url, 2)─┐
│ clickhouse.com                         │ (['ClickHouse'],[1],[],[])                                                            │
│ clickhouse.com/docs/ja                 │ (['ClickHouse Documentation','ClickHouse'],[0,1],['/en'],['ClickHouse'])              │
│ github.com/clickhouse/tree/master/docs │ (['Documentation','GitHub'],[2,3],[NULL],[])                                          │
└────────────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────┘
```

#### マッチングモード

パターンマッチングの動作は特定のディクショナリ設定で変更できます：
- `regexp_dict_flag_case_insensitive`: 大文字小文字を区別しないマッチングを使用（デフォルトは`false`）。個々の表現で`(?i)`と`(?-i)`で上書き可能。
- `regexp_dict_flag_dotall`: `.`を改行文字とマッチさせる（デフォルトは`false`）。

### ClickHouse Cloudでの正規表現ツリーディクショナリの使用

上記の`YAMLRegExpTree`ソースはClickHouseオープンソースでは動作しますが、ClickHouse Cloudでは動作しません。ClickHouse Cloudで正規表現ツリーディクショナリを使用するには、まずローカルのClickHouseオープンソースでYAMLファイルから正規表現ツリーディクショナリを作成し、`dictionaries`テーブル関数と[INTO OUTFILE](../statements/select/into-outfile.md)句を使用してこのディクショナリをCSVファイルにダンプします。

```sql
SELECT * FROM dictionary(regexp_dict) INTO OUTFILE('regexp_dict.csv')
```

csvファイルの内容は以下の通りです：

```text
1,0,"Linux/(\d+[\.\d]*).+tlinux","['version','name']","['\\1','TencentOS']"
2,0,"(\d+)/tclwebkit(\d+[\.\d]*)","['comment','version','name']","['test $1 and $2','$1','Android']"
3,2,"33/tclwebkit","['version']","['13']"
4,2,"3[12]/tclwebkit","['version']","['12']"
5,2,"30/tclwebkit","['version']","['11']"
6,2,"29/tclwebkit","['version']","['10']"
```

ダンプされたファイルのスキーマは次の通りです：

- `id UInt64`: RegexpTreeノードのID。
- `parent_id UInt64`: ノードの親のID。
- `regexp String`: 正規表現文字列。
- `keys Array(String)`: ユーザー定義属性の名前。
- `values Array(String)`: ユーザー定義属性の値。

ClickHouse Cloudでディクショナリを作成するには、まず以下のテーブル構造で`regexp_dictionary_source_table`というテーブルを作成します：

```sql
CREATE TABLE regexp_dictionary_source_table
(
    id UInt64,
    parent_id UInt64,
    regexp String,
    keys   Array(String),
    values Array(String)
) ENGINE=Memory;
```

その後、ローカルCSVを以下のコマンドで更新します：

```bash
clickhouse client \
    --host MY_HOST \
    --secure \
    --password MY_PASSWORD \
    --query "
    INSERT INTO regexp_dictionary_source_table
    SELECT * FROM input ('id UInt64, parent_id UInt64, regexp String, keys Array(String), values Array(String)')
    FORMAT CSV" < regexp_dict.csv
```

さらに詳しくは[ローカルファイルの挿入方法](https://clickhouse.com/docs/ja/integrations/data-ingestion/insert-local-files)をご覧ください。ソーステーブルを初期化後、テーブルソースで正規表現ツリーを作成できます：

``` sql
CREATE DICTIONARY regexp_dict
(
    regexp String,
    name String,
    version String
PRIMARY KEY(regexp)
SOURCE(CLICKHOUSE(TABLE 'regexp_dictionary_source_table'))
LIFETIME(0)
LAYOUT(regexp_tree);
```

## 埋め込みディクショナリ

<SelfManaged />

ClickHouseはジオベースを操作するための組み込み機能を持っています。

これにより、以下が可能です：

- 地域のIDを使用して、希望する言語でその名前を取得。
- 地域のIDを使用して、市、地域、連邦地区、国、大陸のIDを取得。
- ある地域が別の地域の一部であるかどうかを確認。
- 親地域のチェーンを取得。

すべての関数は「トランスロカリティ」をサポートしており、地域の所有に関する異なる視点を同時に使用する能力があります。詳細は、「web analyticsディクショナリを操作するための関数」セクションを参照してください。

内部ディクショナリはデフォルトパッケージでは無効です。
それらを有効にするには、サーバー構成ファイルで`path_to_regions_hierarchy_file`および`path_to_regions_names_files`のパラメータをコメントアウトします。

ジオベースはテキストファイルから読み込まれます。

`regions_hierarchy*.txt`ファイルを`path_to_regions_hierarchy_file`ディレクトリに配置します。この構成パラメータにはデフォルトの地域階層である`regions_hierarchy.txt`ファイルへのパスを含める必要があり、他のファイル（`regions_hierarchy_ua.txt`）は同じディレクトリに配置する必要があります。

`regions_names_*.txt`ファイルを`path_to_regions_names_files`ディレクトリに配置します。

これらのファイルを自分で作成することもできます。ファイル形式は以下の通りです：

`regions_hierarchy*.txt`: TabSeparated（ヘッダーなし）、カラム：

- 地域ID（`UInt32`）
- 親地域ID（`UInt32`）
- 地域タイプ（`UInt8`）：1 - 大陸、3 - 国、4 - 連邦地区、5 - 地域、6 - 市; 他のタイプには値がない
- 人口（`UInt32`）— オプションカラム

`regions_names_*.txt`: TabSeparated（ヘッダーなし）、カラム：

- 地域ID（`UInt32`）
- 地域名（`String`）— タブや改行、エスケープされたものも含めることができません。

RAMに格納するためにフラットな配列が使用されます。このため、IDは100万を超えないようにすべきです。

ディクショナリはサーバーを再起動せずに更新できます。ただし、利用可能なディクショナリのセットは更新されません。
更新のためにファイルの変更時間がチェックされます。ファイルが変更された場合、ディクショナリが更新されます。
変更チェックの間隔は`builtin_dictionaries_reload_interval`パラメータで設定されます。
ディクショナリの更新（最初の使用での読み込みを除く）はクエリをブロックしません。更新中、クエリは古いバージョンのディクショナリを使用します。更新中にエラーが発生した場合、エラーはサーバーログに記録され、クエリは古いバージョンのディクショナリを使用して続行します。

ジオベースを用いてディクショナリを定期的に更新することが推奨されます。更新中、新しいファイルを生成し、それらを別の場所に書き込みます。すべて準備が整ったら、サーバーで使用されるファイルに名前を変更します。

OS識別子や検索エンジンの操作のための関数もありますが、使用すべきでありません。
