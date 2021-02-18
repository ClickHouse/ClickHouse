---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u4E00\u822C\u7684\u306A\u8AAC\u660E"
---

# 外部辞書 {#dicts-external-dicts}

さまざまなデータソースから独自の辞書を追加できます。 ディクショナリのデータソースには、ローカルテキストまたは実行可能ファイル、HTTPリソース、または別のDBMSを使用できます。 詳細については、 “[外部辞書のソース](external-dicts-dict-sources.md)”.

クリックハウス:

-   完全または部分的にRAMに辞書を格納します。
-   辞書を定期的に更新し、欠損値を動的に読み込みます。 つまり、辞書は動的に読み込むことができます。
-   Xmlファイルで外部辞書を作成することができます。 [DDLクエリ](../../statements/create.md#create-dictionary-query).

外部辞書の構成は、一つ以上のxmlファイルに配置できます。 設定へのパスは [dictionaries\_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config) パラメータ。

辞書は、サーバーの起動時または最初の使用時に読み込むことができます。 [dictionaries\_lazy\_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) 設定。

その [辞書](../../../operations/system-tables.md#system_tables-dictionaries) システムテーブルについての情報が含まれて辞書に設定されます。 各辞書については、そこにあります:

-   辞書の状態。
-   設定パラメータ。
-   メトリクスのような量のメモリ割り当てのための辞書は多数のクエリからの辞書に成功したとみなされます。

辞書構成ファイルの形式は次のとおりです:

``` xml
<yandex>
    <comment>An optional element with any content. Ignored by the ClickHouse server.</comment>

    <!--Optional element. File name with substitutions-->
    <include_from>/etc/metrika.xml</include_from>


    <dictionary>
        <!-- Dictionary configuration. -->
        <!-- There can be any number of <dictionary> sections in the configuration file. -->
    </dictionary>

</yandex>
```

あなたはできる [設定](external-dicts-dict.md) 同じファイル内の任意の数の辞書。

[辞書のDDLクエリ](../../statements/create.md#create-dictionary-query) サーバー構成に追加のレコードは必要ありません。 この仕事を辞書として第一級の体のように、テーブルやビュー。

!!! attention "注意"
    小さな辞書の値を変換するには、次のように記述します `SELECT` クエリ（参照 [変換](../../../sql-reference/functions/other-functions.md) 機能）。 この機能は外部辞書とは関係ありません。

## も参照。 {#ext-dicts-see-also}

-   [外部ディクショナリの構成](external-dicts-dict.md)
-   [メモリへの辞書の格納](external-dicts-dict-layout.md)
-   [辞書の更新](external-dicts-dict-lifetime.md)
-   [外部辞書のソース](external-dicts-dict-sources.md)
-   [辞書キーとフィールド](external-dicts-dict-structure.md)
-   [外部辞書を操作するための関数](../../../sql-reference/functions/ext-dict-functions.md)

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts/) <!--hide-->
