---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u4E00\u822C\u7684\u306A\u8AAC\u660E"
---

# 外部辞書 {#dicts-external-dicts}

さまざまなデータソースから独自の辞書を追加できます。 ディクショナリのデータソースには、ローカルテキストまたは実行可能ファイル、http(s)リソース、または別のdbmsを指定できます。 詳細については、 “[外部辞書のソース](external-dicts-dict-sources.md)”.

クリックハウス:

-   完全または部分的にramに辞書を格納します。
-   辞書を定期的に更新し、欠損値を動的に読み込みます。 つまり、辞書を動的に読み込むことができます。
-   Xmlファイルを使用して外部辞書を作成したり [DDLクエリ](../../statements/create.md#create-dictionary-query).

外部辞書の設定は、一つ以上のxmlファイルに配置することができます。 設定へのパスは [dictionaries\_config](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_config) パラメータ。

辞書は、サーバーの起動時または最初の使用時にロードすることができます。 [dictionaries\_lazy\_load](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-dictionaries_lazy_load) 設定。

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

[辞書のddlクエリ](../../statements/create.md#create-dictionary-query) サーバー構成に追加のレコードは必要ありません。 この仕事を辞書として第一級の体のように、テーブルやビュー。

!!! attention "注意"
    小さな辞書の値をaに記述することによって変換できます `SELECT` クエリ(参照 [変換](../../../sql-reference/functions/other-functions.md) 機能）。 この機能は外部辞書とは関係ありません。

## また見なさい {#ext-dicts-see-also}

-   [外部ディクショナリの設定](external-dicts-dict.md)
-   [辞書をメモリに保存する](external-dicts-dict-layout.md)
-   [辞書の更新](external-dicts-dict-lifetime.md)
-   [外部辞書のソース](external-dicts-dict-sources.md)
-   [辞書のキーとフィールド](external-dicts-dict-structure.md)
-   [外部辞書を操作するための関数](../../../sql-reference/functions/ext-dict-functions.md)

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/external_dicts/) <!--hide-->
