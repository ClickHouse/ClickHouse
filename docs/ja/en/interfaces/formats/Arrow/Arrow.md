---
alias: []
description: 'Arrow フォーマットのドキュメント'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[Apache Arrow](https://arrow.apache.org/) には、組み込みの列指向ストレージフォーマットが 2 つあります。
ClickHouse は、これらのフォーマットの読み書きをサポートしています。
`Arrow` は Apache Arrow の「ファイルモード」フォーマットで、メモリ内でのランダムアクセス向けに設計されています。

<div id="data-types-matching">
  ## データ型の対応
</div>

以下の表は、サポートされているデータ型と、それぞれが `INSERT` および `SELECT` クエリで ClickHouse の [データ型](/ja/sql-reference/data-types/index.md) にどのように対応するかを示しています。

| Arrow データ型 (`INSERT`)                   | ClickHouse データ型                                                                                                  | Arrow データ型 (`SELECT`) |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------- |
| `BOOL`                                  | [Bool](/ja/sql-reference/data-types/boolean.md)                                                                     | `BOOL`                |
| `UINT8`, `BOOL`                         | [UInt8](/ja/sql-reference/data-types/int-uint.md)                                                                   | `UINT8`               |
| `INT8`                                  | [Int8](/ja/sql-reference/data-types/int-uint.md)/[Enum8](/ja/sql-reference/data-types/enum.md)                         | `INT8`                |
| `UINT16`                                | [UInt16](/ja/sql-reference/data-types/int-uint.md)                                                                  | `UINT16`              |
| `INT16`                                 | [Int16](/ja/sql-reference/data-types/int-uint.md)/[Enum16](/ja/sql-reference/data-types/enum.md)                       | `INT16`               |
| `UINT32`                                | [UInt32](/ja/sql-reference/data-types/int-uint.md)                                                                  | `UINT32`              |
| `INT32`                                 | [Int32](/ja/sql-reference/data-types/int-uint.md)                                                                   | `INT32`               |
| `UINT64`                                | [UInt64](/ja/sql-reference/data-types/int-uint.md)                                                                  | `UINT64`              |
| `INT64`                                 | [Int64](/ja/sql-reference/data-types/int-uint.md)                                                                   | `INT64`               |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/ja/sql-reference/data-types/float.md)                                                                    | `FLOAT32`             |
| `DOUBLE`                                | [Float64](/ja/sql-reference/data-types/float.md)                                                                    | `FLOAT64`             |
| `DATE32`                                | [Date32](/ja/sql-reference/data-types/date32.md)                                                                    | `UINT16`              |
| `DATE64`                                | [DateTime](/ja/sql-reference/data-types/datetime.md)                                                                | `UINT32`              |
| `TIMESTAMP`                             | [DateTime64](/ja/sql-reference/data-types/datetime64.md)                                                            | `TIMESTAMP`           |
| `TIME32`, `TIME64`                      | [Time64](/ja/sql-reference/data-types/time64.md)                                                                    | `TIME32`, `TIME64`    |
| `STRING`, `BINARY`                      | [String](/ja/sql-reference/data-types/string.md)                                                                    | `BINARY`              |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/ja/sql-reference/data-types/fixedstring.md)                                                          | `FIXED_SIZE_BINARY`   |
| `DECIMAL`                               | [Decimal](/ja/sql-reference/data-types/decimal.md)                                                                  | `DECIMAL`             |
| `DECIMAL256`                            | [Decimal256](/ja/sql-reference/data-types/decimal.md)                                                               | `DECIMAL256`          |
| `LIST`                                  | [Array](/ja/sql-reference/data-types/array.md)                                                                      | `LIST`                |
| `STRUCT`                                | [Tuple](/ja/sql-reference/data-types/tuple.md)                                                                      | `STRUCT`              |
| `MAP`                                   | [Map](/ja/sql-reference/data-types/map.md)                                                                          | `MAP`                 |
| `UINT32`                                | [IPv4](/ja/sql-reference/data-types/ipv4.md)                                                                        | `UINT32`              |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/ja/sql-reference/data-types/ipv6.md)                                                                        | `FIXED_SIZE_BINARY`   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/ja/sql-reference/data-types/int-uint.md)                                           | `FIXED_SIZE_BINARY`   |
| `DURATION`                              | [Interval](/ja/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`            |
| `INT64`                                 | [Interval](/ja/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year)   | `INT64`               |

Array はネストでき、引数に `Nullable` 型の値を取ることもできます。`Tuple` 型と `Map` 型もネストできます。

`DICTIONARY` 型は `INSERT` クエリでサポートされています。また、`SELECT` クエリには [`output_format_arrow_low_cardinality_as_dictionary`](/ja/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) という設定があり、[LowCardinality](/ja/sql-reference/data-types/lowcardinality.md) 型を `DICTIONARY` 型として出力できます。`LowCardinality` の Dictionary には未使用の値が含まれることがあり、そのため出力時の Arrow `DICTIONARY` にも未使用の値が含まれる可能性がある点に注意してください。

サポートされていない Arrow データ型:

* `FIXED_SIZE_BINARY`
* `JSON`
* `UUID`
* `ENUM`.

ClickHouseテーブルのカラムのデータ型は、対応する Arrow のデータフィールドと一致している必要はありません。データを挿入する際、ClickHouse はまず上記の表に従ってデータ型を解釈し、その後、データを ClickHouseテーブルのカラムに設定されたデータ型に [CAST](/ja/sql-reference/functions/type-conversion-functions#CAST) します。

<div id="example-usage">
  ## 使用例
</div>

以下の例では、
[ClickHouse SQL playground](https://sql.clickhouse.com)で利用できる `forex` データセットを使用します。

<div id="selecting-data">
  ### データの取得
</div>

プレイグラウンドから `EUR/USD` の1日分の為替レートを取得し、
ローカルの `forex_eurusd.arrow` ファイルに保存します。HTTP
インターフェイス経由でプレイグラウンドにクエリを送信します。ホストは
`sql-clickhouse.clickhouse.com`、ユーザーは `demo`
(パスワードなし) です:

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

<div id="reading-data">
  ### ファイルを読み戻す
</div>

これで、[`clickhouse-local`](/ja/operations/utilities/clickhouse-local) の
[`file`](/ja/sql-reference/table-functions/file) テーブル関数を使って、
ローカルの Arrow ファイルを読み戻せます。ファイルは自己記述型のため、
`Arrow` フォーマットがスキーマを自動的に推論します。

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

<div id="inserting-data">
  ### データの挿入
</div>

ClickHouseテーブルにArrowファイルを読み込むには、`FORMAT Arrow` を指定して
`clickhouse-client` にパイプで渡します:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                           | 説明                                                                          | デフォルト       |
| ---------------------------------------------------------------------------- | --------------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | Arrow 入力フォーマットの読み取り時に、欠落しているカラムを許可します                                       | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Arrow のカラムと CH columns を照合する際に、大文字と小文字を区別しません。                              | `0`         |
| `input_format_arrow_import_nested`                                           | 廃止された設定で、効果はありません。                                                          | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Arrow フォーマットのスキーマ推論時に、サポートされていない型のカラムをスキップします                               | `0`         |
| `output_format_arrow_compression_method`                                     | Arrow 出力フォーマットの圧縮方式です。対応コーデック: lz4&#95;frame, zstd, none (非圧縮)              | `lz4_frame` |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | FixedString 型のカラムでは、Binary の代わりに Arrow の FIXED&#95;SIZE&#95;BINARY 型を使用します。 | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | LowCardinality 型を Dictionary Arrow 型として出力します                                | `0`         |
| `output_format_arrow_string_as_string`                                       | String 型のカラムでは、Binary の代わりに Arrow String 型を使用します                            | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Arrow フォーマットの dictionary indexes には常に 64 ビット整数を使用します                        | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Arrow フォーマットの dictionary indexes に符号付き整数を使用します                              | `1`         |