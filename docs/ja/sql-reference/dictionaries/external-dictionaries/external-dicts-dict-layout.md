---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 41
toc_title: "\u8F9E\u66F8\u3092\u30E1\u30E2\u30EA\u306B\u4FDD\u5B58\u3059\u308B"
---

# 辞書をメモリに保存する {#dicts-external-dicts-dict-layout}

辞書をメモリに保存するには、さまざまな方法があります。

私たちはお勧め [フラット](#flat), [ハッシュ](#dicts-external_dicts_dict_layout-hashed) と [complex\_key\_hashed](#complex-key-hashed). 最適の処理速度を提供するかどれが。

キャッシュ推奨されていないものになる可能性のある性能や困難の選定に最適なパラメータ。 セクションの続きを読む “[キャッシュ](#cache)”.

辞書のパフォーマ:

-   後に辞書を操作する関数を呼び出します `GROUP BY`.
-   射影として抽出する属性をマークします。 異なる属性値が異なるキーに対応する場合、属性はinjectiveと呼ばれます。 だからとき `GROUP BY` キーによって属性値をフェッチする関数を使用します。 `GROUP BY`.

ClickHouseは、辞書のエラーの例外を生成します。 エラーの例:

-   アクセス中の辞書を読み込めませんでした。
-   エラーの照会 `cached` 辞書だ

外部辞書のリストとそのステータスを表示することができます `system.dictionaries` テーブル。

構成は次のようになります:

``` xml
<yandex>
    <dictionary>
        ...
        <layout>
            <layout_type>
                <!-- layout settings -->
            </layout_type>
        </layout>
        ...
    </dictionary>
</yandex>
```

対応 [DDL-クエリ](../../statements/create.md#create-dictionary-query):

``` sql
CREATE DICTIONARY (...)
...
LAYOUT(LAYOUT_TYPE(param value)) -- layout settings
...
```

## 辞書をメモリに格納する方法 {#ways-to-store-dictionaries-in-memory}

-   [フラット](#flat)
-   [ハッシュ](#dicts-external_dicts_dict_layout-hashed)
-   [sparse\_hashed](#dicts-external_dicts_dict_layout-sparse_hashed)
-   [キャッシュ](#cache)
-   [range\_hashed](#range-hashed)
-   [complex\_key\_hashed](#complex-key-hashed)
-   [complex\_key\_cache](#complex-key-cache)
-   [ip\_trie](#ip-trie)

### フラット {#flat}

辞書は完全にフラット配列の形でメモリに格納されています。 辞書はどのくらいのメモリを使用しますか？ この量は、最大のキーのサイズに比例します（使用されるスペース）。

辞書キーには、 `UInt64` 値は500,000に制限されます。 辞書を作成するときに大きなキーが検出された場合、ClickHouseは例外をスローし、辞書を作成しません。

すべてのタイプの源は支えられる。 更新時には、ファイルまたはテーブルからのデータが完全に読み取られます。

この方法は最高性能の中で利用可能なすべての方法を格納する辞書です。

構成例:

``` xml
<layout>
  <flat />
</layout>
```

または

``` sql
LAYOUT(FLAT())
```

### ハッシュ {#dicts-external_dicts_dict_layout-hashed}

辞書は、ハッシュテーブルの形式でメモリに完全に格納されます。 辞書には、実際には任意の識別子を持つ任意の数の要素を含めることができ、キーの数は数千万のアイテムに達することができます。

すべてのタイプの源は支えられる。 更新時には、ファイルまたはテーブルからのデータが完全に読み取られます。

構成例:

``` xml
<layout>
  <hashed />
</layout>
```

または

``` sql
LAYOUT(HASHED())
```

### sparse\_hashed {#dicts-external_dicts_dict_layout-sparse_hashed}

に似て `hashed` が、使用メモリ賛以上のCPUます。

構成例:

``` xml
<layout>
  <sparse_hashed />
</layout>
```

``` sql
LAYOUT(SPARSE_HASHED())
```

### complex\_key\_hashed {#complex-key-hashed}

このタイプの貯蔵は合成の使用のためです [キー](external-dicts-dict-structure.md). に似て `hashed`.

構成例:

``` xml
<layout>
  <complex_key_hashed />
</layout>
```

``` sql
LAYOUT(COMPLEX_KEY_HASHED())
```

### range\_hashed {#range-hashed}

辞書は、範囲とそれに対応する値の順序付けられた配列を持つハッシュテーブルの形式でメモリに格納されます。

このストレージメソッドは、hashedと同じように機能し、キーに加えて日付/時刻（任意の数値型）の範囲を使用できます。

例:この表には、各広告主の割引の形式が含まれています:

``` text
+---------|-------------|-------------|------+
| advertiser id | discount start date | discount end date | amount |
+===============+=====================+===================+========+
| 123           | 2015-01-01          | 2015-01-15        | 0.15   |
+---------|-------------|-------------|------+
| 123           | 2015-01-16          | 2015-01-31        | 0.25   |
+---------|-------------|-------------|------+
| 456           | 2015-01-01          | 2015-01-15        | 0.05   |
+---------|-------------|-------------|------+
```

日付範囲のサンプルを使用するには、以下を定義します `range_min` と `range_max` の要素 [構造](external-dicts-dict-structure.md). これらの要素の要素が含まれている必要があ `name` と`type` （もし `type` デフォルトのタイプが使用されます-Date)。 `type` 任意の数値型(Date/DateTime/UInt64/Int32/others)を指定できます。

例えば:

``` xml
<structure>
    <id>
        <name>Id</name>
    </id>
    <range_min>
        <name>first</name>
        <type>Date</type>
    </range_min>
    <range_max>
        <name>last</name>
        <type>Date</type>
    </range_max>
    ...
```

または

``` sql
CREATE DICTIONARY somedict (
    id UInt64,
    first Date,
    last Date
)
PRIMARY KEY id
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last)
```

これらの辞書を操作するには、追加の引数を渡す必要があります `dictGetT` 範囲が選択される関数:

``` sql
dictGetT('dict_name', 'attr_name', id, date)
```

この関数は、指定された値を返します `id`sと、渡された日付を含む日付範囲。

アルゴリズムの詳細:

-   この `id` が見つからないか、または範囲が見つからない。 `id`、辞書のデフォルト値を返します。
-   範囲が重複している場合は、anyを使用できます。
-   範囲区切り文字が次の場合 `NULL` または無効な日付(1900-01-01や2039-01-01など)では、範囲は開いたままになります。 範囲は両側で開きます。

構成例:

``` xml
<yandex>
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
</yandex>
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

### キャッシュ {#cache}

辞書は、固定数のセルを持つキャッシュに格納されます。 これらの細胞を含む使用頻度の高います。

辞書を検索するときは、まずキャッシュが検索されます。 各ブロックのデータは、すべてのキーが見つからないのキャッシュ古いから要求されてソースを使用 `SELECT attrs... FROM db.table WHERE id IN (k1, k2, ...)`. 受信したデータは、キャッシュに書き込まれます。

キャッシュ辞書の場合、有効期限 [寿命](external-dicts-dict-lifetime.md) キャッシュ内のデータの設定が可能です。 より多くの時間より `lifetime` セルにデータをロードしてから、セルの値は使用されず、次に使用する必要があるときに再要求されます。
これは、辞書を格納するすべての方法の中で最も効果的ではありません。 キャッシュの速度は、正しい設定と使用シナリオに強く依存します。 キャッシュタイプの辞書は、ヒット率が十分に高い場合にのみうまく機能します（推奨99％以上）。 あなたは平均ヒット率を表示することができます `system.dictionaries` テーブル。

キャッシュパフォーマン `LIMIT`、および外部ディクショナリで関数を呼び出します。

サポート [ソース](external-dicts-dict-sources.md)：MySQL、ClickHouse、実行可能ファイル、HTTP。

設定例:

``` xml
<layout>
    <cache>
        <!-- The size of the cache, in number of cells. Rounded up to a power of two. -->
        <size_in_cells>1000000000</size_in_cells>
    </cache>
</layout>
```

または

``` sql
LAYOUT(CACHE(SIZE_IN_CELLS 1000000000))
```

十分なキャッシュサイズを設定します。 あなたは、セルの数を選択する実験する必要があります:

1.  値を設定します。
2.  キャッシュが一杯になるまでクエリを実行します。
3.  を使用してメモリ消費量を評価する `system.dictionaries` テーブル。
4.  必要なメモリ消費量に達するまで、セル数を増減します。

!!! warning "警告"
    ランダムな読み取りでクエリを処理するのが遅いため、ソースとしてclickhouseを使用しないでください。

### complex\_key\_cache {#complex-key-cache}

このタイプの貯蔵は合成の使用のためです [キー](external-dicts-dict-structure.md). に似て `cache`.

### ip\_trie {#ip-trie}

このタイプの貯蔵するマッピングするネットワーク接頭辞(ipアドレスへのメタデータなどのasn.

例:テーブルを含むネットワークの接頭辞およびその対応としての数および国コード:

``` text
  +-----------|-----|------+
  | prefix          | asn   | cca2   |
  +=================+=======+========+
  | 202.79.32.0/20  | 17501 | NP     |
  +-----------|-----|------+
  | 2620:0:870::/48 | 3856  | US     |
  +-----------|-----|------+
  | 2a02:6b8:1::/48 | 13238 | RU     |
  +-----------|-----|------+
  | 2001:db8::/32   | 65536 | ZZ     |
  +-----------|-----|------+
```

このタイプのレイアウトを使用する場合、構造に複合キーが必要です。

例えば:

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
```

または

``` sql
CREATE DICTIONARY somedict (
    prefix String,
    asn UInt32,
    cca2 String DEFAULT '??'
)
PRIMARY KEY prefix
```

キーには、許可されたipプレフィックスを含む文字列型属性のみが必要です。 その他のタイプはサポートされていませんか。

クエリの場合は、同じ関数を使用する必要があります (`dictGetT` 複合キーを持つ辞書については、タプルを使用する:

``` sql
dictGetT('dict_name', 'attr_name', tuple(ip))
```

のいずれか `UInt32` IPv4の場合、または `FixedString(16)` IPv6の場合:

``` sql
dictGetString('prefix', 'asn', tuple(IPv6StringToNum('2001:db8::1')))
```

その他のタイプはサポートされていませんか。 この関数は、このipアドレスに対応するプレフィックスの属性を返します。 がある場合に重なる接頭辞であり、具体的には返却されます。

データは `trie`. それは完全にRAMに収まる必要があります。

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict_layout/) <!--hide-->
