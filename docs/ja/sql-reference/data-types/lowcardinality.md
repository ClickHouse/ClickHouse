---
slug: /ja/sql-reference/data-types/lowcardinality
sidebar_position: 42
sidebar_label: LowCardinality(T)
---

# LowCardinality(T)

他のデータ型の内部表現をDictionaryエンコードで変更します。

## 構文

``` sql
LowCardinality(data_type)
```

**パラメータ**

- `data_type` — [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md)、および [Decimal](../../sql-reference/data-types/decimal.md) を除く数値。 `LowCardinality` は一部のデータ型には効率的でない場合があります。[allow_suspicious_low_cardinality_types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types) の設定説明を参照してください。

## 説明

`LowCardinality` はデータの保存方法と処理ルールを変更する上位構造です。ClickHouse は`LowCardinality`-カラムに [Dictionary コーディング](https://en.wikipedia.org/wiki/Dictionary_coder) を適用します。Dictionary エンコードされたデータを操作すると、多くのアプリケーションで [SELECT](../../sql-reference/statements/select/index.md) クエリのパフォーマンスが大幅に向上します。

`LowCardinality` データ型の使用効率は、データの多様性に依存します。Dictionary に1万未満の異なる値が含まれる場合、ClickHouse は主にデータの読み取りと保存の効率が高くなります。Dictionary に10万を超える異なる値が含まれる場合、通常のデータ型を使用する方が性能が低下する可能性があります。

文字列を扱う際には、[Enum](../../sql-reference/data-types/enum.md) の代わりに `LowCardinality` の使用を検討してください。`LowCardinality` はより柔軟に使用でき、しばしば同等またはそれ以上の効率を発揮します。

## 例

`LowCardinality`-カラムを持つテーブルを作成します:

``` sql
CREATE TABLE lc_t
(
    `id` UInt16,
    `strings` LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY id
```

## 関連する設定と関数

設定:

- [low_cardinality_max_dictionary_size](../../operations/settings/settings.md#low_cardinality_max_dictionary_size)
- [low_cardinality_use_single_dictionary_for_part](../../operations/settings/settings.md#low_cardinality_use_single_dictionary_for_part)
- [low_cardinality_allow_in_native_format](../../operations/settings/settings.md#low_cardinality_allow_in_native_format)
- [allow_suspicious_low_cardinality_types](../../operations/settings/settings.md#allow_suspicious_low_cardinality_types)
- [output_format_arrow_low_cardinality_as_dictionary](../../operations/settings/settings.md#output-format-arrow-low-cardinality-as-dictionary)

関数:

- [toLowCardinality](../../sql-reference/functions/type-conversion-functions.md#tolowcardinality)

## 関連コンテンツ

- ブログ: [スキーマとコーデックでClickHouseを最適化する](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
- ブログ: [ClickHouseでタイムシリーズデータを扱う](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
- [文字列最適化 (ロシア語のビデオプレゼンテーション)](https://youtu.be/rqf-ILRgBdY?list=PL0Z2YDlm0b3iwXCpEFiOOYmwXzVmjJfEt). [英語のスライド](https://github.com/ClickHouse/clickhouse-presentations/raw/master/meetup19/string_optimization.pdf)
