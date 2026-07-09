---
description: 'String 型のカラムに対する LowCardinality 最適化のドキュメント'
sidebar_label: 'LowCardinality(T)'
sidebar_position: 42
slug: /sql-reference/data-types/lowcardinality
title: 'LowCardinality(T)'
doc_type: 'reference'
---

他のデータ型の内部表現を辞書エンコードに変更します。

<div id="syntax">
  ## 構文
</div>

```sql
LowCardinality(data_type)
```

**パラメータ**

* `data_type` — [String](../../sql-reference/data-types/string.md)、[FixedString](../../sql-reference/data-types/fixedstring.md)、[Date](../../sql-reference/data-types/date.md)、[DateTime](../../sql-reference/data-types/datetime.md)、および [Decimal](../../sql-reference/data-types/decimal.md) を除く数値型。`LowCardinality` は一部のデータ型では効率がよくないため、[allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types) 設定の説明を参照してください。

<div id="description">
  ## 説明
</div>

`LowCardinality` は、データの保存方法と処理ルールを変更する上位の仕組みです。ClickHouse は `LowCardinality` カラムに [辞書符号化](https://en.wikipedia.org/wiki/Dictionary_coder) を適用します。辞書符号化されたデータを扱うことで、多くのアプリケーションで [SELECT](../../sql-reference/statements/select/index.md) クエリのパフォーマンスが大幅に向上します。

`LowCardinality` データ型の効果は、データの多様性に依存します。辞書に含まれる異なる値が 10,000 未満であれば、ClickHouse では一般にデータ読み取りと保存の効率が高くなります。辞書に含まれる異なる値が 100,000 を超える場合は、通常のデータ型を使用する場合と比べて、かえってパフォーマンスが低下することがあります。

文字列を扱う場合は、[Enum](../../sql-reference/data-types/enum.md) の代わりに `LowCardinality` の使用を検討してください。`LowCardinality` はより柔軟性が高く、同等以上の効率が得られることも少なくありません。

<div id="example">
  ## 例
</div>

`LowCardinality` カラムを持つテーブルを作成します:

```sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

<div id="related-settings-and-functions">
  ## 関連する設定と関数
</div>

設定:

* [low&#95;cardinality&#95;max&#95;dictionary&#95;size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
* [low&#95;cardinality&#95;use&#95;single&#95;dictionary&#95;for&#95;part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
* [low&#95;cardinality&#95;allow&#95;in&#95;native&#95;format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
* [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
* [output&#95;format&#95;arrow&#95;low&#95;cardinality&#95;as&#95;dictionary](/ja/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary)

関数:

* [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#toLowCardinality)

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [スキーマとコーデックを使って ClickHouse を最適化する](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* ブログ: [ClickHouse で時系列データを扱う](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* [String の最適化 (ロシア語によるビデオプレゼンテーション) ](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt)。[英語のスライド](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)