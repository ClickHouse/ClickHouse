---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u8F9E\u66F8"
toc_priority: 35
toc_title: "\u306F\u3058\u3081\u306B"
---

# 辞書 {#dictionaries}

辞書はマッピングです (`key -> attributes`）それはさまざまなタイプの参照リストのために便利です。

ClickHouseは、クエリで使用できる辞書を操作するための特別な関数をサポートしています。 関数で辞書を使用する方が簡単で効率的です。 `JOIN` 参照のテーブルを使って。

[NULL](../../sql-reference/syntax.md#null-literal) 値を辞書に格納することはできません。

ClickHouseサポート:

-   [組み込み辞書](internal-dicts.md#internal_dicts) 特定の [関数のセット](../../sql-reference/functions/ym-dict-functions.md).
-   [プラグイン(外部)辞書](external-dictionaries/external-dicts.md#dicts-external-dicts) と [関数のセット](../../sql-reference/functions/ext-dict-functions.md).

[元の記事](https://clickhouse.tech/docs/en/query_language/dicts/) <!--hide-->
