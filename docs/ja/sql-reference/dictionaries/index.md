---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_folder_title: Dictionaries
toc_priority: 35
toc_title: "\u5C0E\u5165"
---

# 辞書 {#dictionaries}

辞書はマッピングです (`key -> attributes`）それはさまざまなタイプの参照リストのために便利です。

ClickHouseは、クエリで使用できる辞書を操作するための特別な機能をサポートしています。 Aよりも関数で辞書を使用する方が簡単で効率的です `JOIN` 参照テーブルと。

[NULL](../../sql-reference/syntax.md#null-literal) 値を辞書に格納することはできません。

ClickHouse支援:

-   [内蔵の辞書](internal-dicts.md#internal_dicts) 特定の [関数のセット](../../sql-reference/functions/ym-dict-functions.md).
-   [プラグイン（外部）辞書](external-dictionaries/external-dicts.md#dicts-external-dicts) と [機能のネット](../../sql-reference/functions/ext-dict-functions.md).

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
