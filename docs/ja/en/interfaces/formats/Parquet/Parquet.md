---
alias: []
description: 'Parquetフォーマットのドキュメント'
input_format: true
keywords: ['Parquet']
output_format: true
slug: /interfaces/formats/Parquet
title: 'Parquet'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

[Apache Parquet](https://parquet.apache.org/) は、Hadoop エコシステムで広く使われている列指向のストレージフォーマットです。ClickHouse は、このフォーマットの読み取りと書き込みをサポートしています。

<div id="data-types-matching-parquet">
  ## データ型の対応
</div>

以下の表は、Parquet のデータ型が ClickHouse の[データ型](/ja/sql-reference/data-types/index.md)にどのように対応しているかを示します。

| Parquet type (logical, converted, or physical) | ClickHouse data type                                                                       |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `BOOLEAN`                                      | [Bool](/ja/sql-reference/data-types/boolean.md)                                               |
| `UINT_8`                                       | [UInt8](/ja/sql-reference/data-types/int-uint.md)                                             |
| `INT_8`                                        | [Int8](/ja/sql-reference/data-types/int-uint.md)                                              |
| `UINT_16`                                      | [UInt16](/ja/sql-reference/data-types/int-uint.md)                                            |
| `INT_16`                                       | [Int16](/ja/sql-reference/data-types/int-uint.md)/[Enum16](/ja/sql-reference/data-types/enum.md) |
| `UINT_32`                                      | [UInt32](/ja/sql-reference/data-types/int-uint.md)                                            |
| `INT_32`                                       | [Int32](/ja/sql-reference/data-types/int-uint.md)                                             |
| `UINT_64`                                      | [UInt64](/ja/sql-reference/data-types/int-uint.md)                                            |
| `INT_64`                                       | [Int64](/ja/sql-reference/data-types/int-uint.md)                                             |
| `DATE`                                         | [Date32](/ja/sql-reference/data-types/date.md)                                                |
| `TIMESTAMP`, `TIME`                            | [DateTime64](/ja/sql-reference/data-types/datetime64.md)                                      |
| `FLOAT`                                        | [Float32](/ja/sql-reference/data-types/float.md)                                              |
| `DOUBLE`                                       | [Float64](/ja/sql-reference/data-types/float.md)                                              |
| `INT96`                                        | [DateTime64(9, &#39;UTC&#39;)](/ja/sql-reference/data-types/datetime64.md)                    |
| `BYTE_ARRAY`, `UTF8`, `ENUM`, `BSON`           | [String](/ja/sql-reference/data-types/string.md)                                              |
| `JSON`                                         | [JSON](/ja/sql-reference/data-types/newjson.md)                                               |
| `FIXED_LEN_BYTE_ARRAY`                         | [FixedString](/ja/sql-reference/data-types/fixedstring.md)                                    |
| `DECIMAL`                                      | [Decimal](/ja/sql-reference/data-types/decimal.md)                                            |
| `LIST`                                         | [Array](/ja/sql-reference/data-types/array.md)                                                |
| `MAP`                                          | [Map](/ja/sql-reference/data-types/map.md)                                                    |
| struct                                         | [Tuple](/ja/sql-reference/data-types/tuple.md)                                                |
| `FLOAT16`                                      | [Float32](/ja/sql-reference/data-types/float.md)                                              |
| `UUID`                                         | [FixedString(16)](/ja/sql-reference/data-types/fixedstring.md)                                |
| `INTERVAL`                                     | [FixedString(12)](/ja/sql-reference/data-types/fixedstring.md)                                |
| `Point` (GeoParquet)                           | [Point](/ja/sql-reference/data-types/geo.md#point)                                            |
| `LineString` (GeoParquet)                      | [LineString](/ja/sql-reference/data-types/geo.md#linestring)                                  |
| `Polygon` (GeoParquet)                         | [Polygon](/ja/sql-reference/data-types/geo.md#polygon)                                        |
| `MultiLineString` (GeoParquet)                 | [MultiLineString](/ja/sql-reference/data-types/geo.md#multilinestring)                        |
| `MultiPolygon` (GeoParquet)                    | [MultiPolygon](/ja/sql-reference/data-types/geo.md#multipolygon)                              |
| 混在または不明なジオメトリ (GeoParquet)                     | [Geometry](/ja/sql-reference/data-types/geo.md#geometry)                                      |

Parquet ファイルへの書き込み時に、対応する Parquet 型がないデータ型は、利用可能な最も近い型に変換されます:

| ClickHouse データ型                                                        | Parquet 型                                    |
| ---------------------------------------------------------------------- | -------------------------------------------- |
| [IPv4](/ja/sql-reference/data-types/ipv4.md)                              | `UINT_32`                                    |
| [IPv6](/ja/sql-reference/data-types/ipv6.md)                              | `FIXED_LEN_BYTE_ARRAY` (16 バイト)              |
| [Date](/ja/sql-reference/data-types/date.md) (16 ビット)                     | `DATE` (32 ビット)                              |
| [DateTime](/ja/sql-reference/data-types/datetime.md) (32 ビット、秒)           | `TIMESTAMP` (64 ビット、ミリ秒)                     |
| [Int128/UInt128/Int256/UInt256](/ja/sql-reference/data-types/int-uint.md) | `FIXED_LEN_BYTE_ARRAY` (16/32 バイト、リトルエンディアン) |
| [Point](/ja/sql-reference/data-types/geo.md#point)                        | `BYTE_ARRAY` (WKB) + GeoParquet メタデータ        |
| [LineString](/ja/sql-reference/data-types/geo.md#linestring)              | `BYTE_ARRAY` (WKB) + GeoParquet メタデータ        |
| [Polygon](/ja/sql-reference/data-types/geo.md#polygon)                    | `BYTE_ARRAY` (WKB) + GeoParquet メタデータ        |
| [MultiLineString](/ja/sql-reference/data-types/geo.md#multilinestring)    | `BYTE_ARRAY` (WKB) + GeoParquet メタデータ        |
| [MultiPolygon](/ja/sql-reference/data-types/geo.md#multipolygon)          | `BYTE_ARRAY` (WKB) + GeoParquet メタデータ        |

Array はネストでき、引数として `Nullable` 型の値を取ることもできます。`Tuple` 型と `Map` 型もネストできます。

ClickHouse テーブルのカラムのデータ型は、挿入される Parquet データ内の対応するフィールドと異なる場合があります。データを挿入する際、ClickHouse はまず上の表に従ってデータ型を解釈し、その後、データを ClickHouse テーブルのカラムに設定されているデータ型に[CAST](/ja/sql-reference/functions/type-conversion-functions#CAST)します。たとえば、`UINT_32` の Parquet カラムは [IPv4](/ja/sql-reference/data-types/ipv4.md) の ClickHouse カラムとして読み取ることができます。

Parquet 型の中には、対応する ClickHouse 型が明確でないものがあります。これらは次のように読み取られます。

* `TIME` (時刻) は timestamp として読み取られます。たとえば、`10:23:13.000` は `1970-01-01 10:23:13.000` になります。
* `TIMESTAMP`/`TIME` で `isAdjustedToUTC=false` の場合、それはローカルの wall-clock time (ローカル timezone における年、月、日、時、分、秒、および秒未満のフィールドで表され、どの time zone がローカルと見なされるかには依存しません) であり、SQL の `TIMESTAMP WITHOUT TIME ZONE` と同じです。ClickHouse はこれを代わりに UTC timestamp であるかのように読み取ります。たとえば、`2025-09-29 18:42:13.000` (ローカルの時計の読みを表す) は `2025-09-29 18:42:13.000` (時点を表す `DateTime64(3, 'UTC')`) になります。String に変換すると、年、月、日、時、分、秒、および秒未満の値は正しく表示されるため、その後で UTC ではなく何らかのローカル timezone の時刻として解釈できます。直感に反しますが、型を `DateTime64(3, 'UTC')` から `DateTime64(3)` に変更しても解決にはなりません。どちらの型も時計の表示ではなく時点を表すためです。ただし、`DateTime64(3)` はローカル timezone を使って誤ってフォーマットされます。
* `INTERVAL` は現在、Parquet file でエンコードされた時間 interval の生のバイナリ表現を持つ `FixedString(12)` として読み取られます。

<div id="geo-types">
  ## Geo 型 (GeoParquet)
</div>

ClickHouse は、[GeoParquet](https://geoparquet.org/) 仕様に準拠した ジオメトリカラム の読み取りと書き込みに対応しています。ジオメトリカラム は、[WKB](https://libgeos.org/specifications/wkb/) (読み取り時は WKT) でエンコードされた `BYTE_ARRAY` ペイロードとして保存され、ファイルレベルの Parquet メタデータ内の JSON `geo` キーには、各 ジオメトリカラム のエンコーディング、ジオメトリ型、CRS が記述されます。

<div id="read">
  ### 読み取り時の動作
</div>

読み取り時には、ジオメトリカラムは対応する ClickHouse の[Geoデータ型](/ja/sql-reference/data-types/geo.md)にマッピングされます。

* `Point`、`LineString`、`Polygon`、`MultiLineString`、`MultiPolygon` として宣言されたカラムは、対応する ClickHouse の Geo 型として読み込まれます。
* 複数のジオメトリ型を含むカラム、またはジオメトリ型が不明なカラムは、サポートされているすべての Geo 型に対する `Variant` である [`Geometry`](/ja/sql-reference/data-types/geo.md#geometry) 型として読み込まれます。
* 要求されたカラム型が `String` の場合、GeoParquet のメタデータは無視され、エンコード済みの生のジオメトリペイロードがそのまま返されます。つまり、GeoParquet カラムで宣言されているエンコーディングに応じて、WKB または WKT のバイト列が返されます。これは、設定 [`input_format_parquet_allow_geoparquet_parser`](/ja/operations/settings/settings-formats.md#input_format_parquet_allow_geoparquet_parser) が `0` に設定されている場合も同様です。

<div id="write">
  ### 書き込み時の動作
</div>

書き込み時には、`Point`、`LineString`、`Polygon`、`MultiLineString`、`MultiPolygon` 型のトップレベルのカラムは `BYTE_ARRAY` (WKB) としてエンコードされ、適切な `geo` JSON メタデータが Parquet ファイルのフッターに追記されます。トップレベルの [`Geometry`](/ja/sql-reference/data-types/geo.md#geometry) `Variant` も WKB の `BYTE_ARRAY` ペイロードとしてエンコードされます (その下位の値は WKB に変換され、`Nullable(String)` カラムとして格納されます) 。ただし、これには `geo` メタデータが出力されないため、読み取り時に GeoParquet のジオメトリカラムとしては認識されません。[`Ring`](/ja/sql-reference/data-types/geo.md#ring) など、その他の geo 関連の型は、GeoParquet メタデータを付けずに、それぞれのネイティブな内部表現で書き込まれます。この動作は、[`output_format_parquet_geometadata`](/ja/operations/settings/settings-formats.md#output_format_parquet_geometadata) を `0` に設定することで完全に無効化できます。この場合は、サポートされている Geo 型であってもネイティブな内部表現 (`Point` は `Tuple(Float64, Float64)`、`LineString` は `Array(Point)`、`Polygon` は `Array(Array(Point))` など) で書き込まれ、GeoParquet メタデータも出力されません。

ジオメトリカラムは、スキーマのルートに配置するか、`Tuple` (`struct`) の中にネストする必要があります。`Array` や `Map` の中にネストすることはサポートされていません。また、geo columns では `Nullable` もサポートされていません。

<div id="example-usage">
  ## 使用例
</div>

<div id="inserting-data">
  ### データの挿入
</div>

次のデータを含む `football.parquet` という名前の Parquetファイルを使用します。

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

データを挿入します:

```sql
INSERT INTO football FROM INFILE 'football.parquet' FORMAT Parquet;
```

<div id="reading-data">
  ### データの読み取り
</div>

`Parquet` フォーマットでデータを読み取ります:

```sql
SELECT *
FROM football
INTO OUTFILE 'football.parquet'
FORMAT Parquet
```

:::tip
Parquetはバイナリ形式のため、ターミナル上で人間が読める形では表示できません。Parquetファイルを出力するには、`INTO OUTFILE` を使用します。
:::

Hadoopとデータをやり取りするには、[`HDFSテーブルエンジン`](/ja/engines/table-engines/integrations/hdfs.md) を使用できます。

<div id="format-settings">
  ## フォーマット設定
</div>

| 設定                                                                             | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 | デフォルト                                                                                                                                                                                                                                                                                                                          |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `input_format_parquet_case_insensitive_column_matching`                        | ParquetのカラムとCH columnsのカラムを照合する際に、大文字と小文字を区別しません。                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | `0`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_preserve_order`                                          | Parquetファイルの読み取り時に行の並べ替えを行いません。通常、これにより大幅に処理が遅くなります。                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `0`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_filter_push_down`                                        | Parquetファイルの読み取り時に、Parquetのメタデータ内のWHERE/PREWHERE式および最小値/最大値の統計に基づいて、行グループ全体をスキップします。                                                                                                                                                                                                                                                                                                                                                                                                                               | `1`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_bloom_filter_push_down`                                  | Parquetファイルの読み取り時に、WHERE式とParquetメタデータ内のbloom filterに基づいて行グループ全体をスキップします。                                                                                                                                                                                                                                                                                                                                                                                                                                          | `0`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_allow_missing_columns`                                   | Parquet入力フォーマットの読み取り時に、欠落しているカラムを許可します                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `1`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_local_file_min_bytes_for_seek`                           | Parquet入力フォーマットで、読み飛ばしながら読む代わりにseekを行うために必要なローカル読み取り (ファイル) の最小バイト数                                                                                                                                                                                                                                                                                                                                                                                                                                                | `8192`                                                                                                                                                                                                                                                                                                                         |
| `input_format_parquet_enable_row_group_prefetch`                               | Parquetのパース中に行グループのプリフェッチを有効にします。現在、プリフェッチできるのはシングルスレッドのパースのみです。                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `1`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference` | Parquet フォーマットのスキーマ推論時に、未対応の型のカラムをスキップします                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `0`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_max_block_size`                                          | Parquet リーダーの最大ブロックサイズ。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `65409`                                                                                                                                                                                                                                                                                                                        |
| `input_format_parquet_prefer_block_bytes`                                      | Parquet リーダーが出力する平均ブロックバイト数                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `16744704`                                                                                                                                                                                                                                                                                                                     |
| `input_format_parquet_enable_json_parsing`                                     | Parquet ファイルの読み取り時に、JSON カラムを ClickHouse JSON Column として解析します。                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `1`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_allow_geoparquet_parser`                                 | Parquetファイルの読み取り時に、GeoParquet の `geo` メタデータを認識し、ジオメトリカラムを (カラムで宣言されたエンコーディングに応じて WKB または WKT から) ClickHouse の Geo データ型としてデコードします。`0` の場合、ジオメトリカラムは物理的な生の表現 (`String`) のまま扱われます。                                                                                                                                                                                                                                                                                                                                    | `1`                                                                                                                                                                                                                                                                                                                            |
| `output_format_parquet_row_group_size`                                         | 目標とする行グループサイズ (行数) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | `1000000`                                                                                                                                                                                                                                                                                                                      |
| `output_format_parquet_row_group_size_bytes`                                   | 圧縮前の目標行グループサイズ (バイト単位) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | `536870912`                                                                                                                                                                                                                                                                                                                    |
| `output_format_parquet_string_as_string`                                       | String 型のカラムには、Binary ではなく Parquet String type を使用します。                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `1`                                                                                                                                                                                                                                                                                                                            |
| `output_format_parquet_fixed_string_as_fixed_byte_array`                       | FixedString カラムには、Binary ではなく Parquet FIXED&#95;LEN&#95;BYTE&#95;ARRAY type を使用します。                                                                                                                                                                                                                                                                                                                                                                                                                                | `1`                                                                                                                                                                                                                                                                                                                            |
| `output_format_parquet_compression_method`                                     | Parquet 出力フォーマットの圧縮方式。サポートされている codecs: snappy, lz4, brotli, zstd, gzip, none (非圧縮)                                                                                                                                                                                                                                                                                                                                                                                                                                | `zstd`                                                                                                                                                                                                                                                                                                                         |
| `output_format_parquet_parallel_encoding`                                      | Parquet のエンコードを複数スレッドで行います。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | `1`                                                                                                                                                                                                                                                                                                                            |
| `output_format_parquet_data_page_size`                                         | 圧縮前の目標ページサイズ (バイト単位) 。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `1048576`                                                                                                                                                                                                                                                                                                                      |
| `output_format_parquet_batch_size`                                             | この行数ごとにページサイズを確認します。平均値のサイズが数 KB を超えるカラムがある場合は、値を小さくすることを検討してください。                                                                                                                                                                                                                                                                                                                                                                                                                                                 | `1024`                                                                                                                                                                                                                                                                                                                         |
| `output_format_parquet_write_page_index`                                       | Parquet ファイルにページ索引を書き込めるようにします。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `1`                                                                                                                                                                                                                                                                                                                            |
| `output_format_parquet_geometadata`                                            | GeoParquet `geo` メタデータを Parquet ファイルのフッターに書き込み、トップレベルの ClickHouse Geo カラム ([`Point`](/ja/sql-reference/data-types/geo.md#point), [`LineString`](/ja/sql-reference/data-types/geo.md#linestring), [`Polygon`](/ja/sql-reference/data-types/geo.md#polygon), [`MultiLineString`](/ja/sql-reference/data-types/geo.md#multilinestring), [`MultiPolygon`](/ja/sql-reference/data-types/geo.md#multipolygon)) を WKB としてエンコードします。`0` の場合、これらのカラムはネイティブの内部表現 (例: `Point` は `Tuple(Float64, Float64)`) のまま書き込まれ、GeoParquet メタデータは出力されません。 | `1`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_import_nested`                                           | 廃止された設定。何も行いません。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | `0`                                                                                                                                                                                                                                                                                                                            |
| `input_format_parquet_local_time_as_utc`                                       | true                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | isAdjustedToUTC=false の Parquet タイムスタンプに対して、スキーマ推論で使用するデータ型を決定します。true の場合: DateTime64(..., &#39;UTC&#39;)、false の場合: DateTime64(...)。ClickHouse にはローカルの wall-clock time 用のデータ型がないため、どちらの動作も完全に正しいわけではありません。直感に反しますが、&#39;true&#39; のほうがおそらくまだ誤りの少ない選択です。これは、&#39;UTC&#39; タイムスタンプを String としてフォーマットすると、正しいローカル時刻の表現が得られるためです。 |