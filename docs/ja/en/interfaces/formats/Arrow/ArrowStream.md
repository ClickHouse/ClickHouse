---
alias: []
description: 'ArrowStreamフォーマットのドキュメント'
input_format: true
keywords: ['ArrowStream']
output_format: true
slug: /interfaces/formats/ArrowStream
title: 'ArrowStream'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

`ArrowStream` は、Apache Arrow の「ストリームモード」フォーマットです。インメモリでのストリーム処理向けに設計されています。

<div id="example-usage">
  ## 使用例
</div>

以下の例では、[ClickHouse SQL playground](https://sql.clickhouse.com) で利用できる `forex` データセットを使用します。`clickhouse-client` を使うと、ホスト `sql-clickhouse.clickhouse.com`、ユーザー `demo` (パスワードなし) でリモート接続できます。`forex` テーブルは `forex` データベースにあるため、これをデフォルトデータベースとして選択します。

```bash
clickhouse-client --secure --host sql-clickhouse.clickhouse.com --user demo --database forex
```

`forex` テーブルには為替レートが保存されています。そのサイズと、
[`system.columns`](/ja/operations/system-tables/columns) にクエリを実行して、
ディスク上でどの程度圧縮効率が高いかを確認できます:

```sql title="Query"
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / sum(data_uncompressed_bytes) AS compression_ratio
FROM system.columns
WHERE (database = 'forex') AND (table = 'forex')
GROUP BY table
ORDER BY table ASC
```

```response title="Response"
   ┌─table─┬─compressed_size─┬─uncompressed_size─┬───compression_ratio─┐
1. │ forex │ 63.69 GiB       │ 280.48 GiB        │ 0.22708227109363446 │
   └───────┴─────────────────┴───────────────────┴─────────────────────┘
```

[`Arrow`](/ja/interfaces/formats/Arrow) の「file mode」フォーマットでは、読み取りを開始する前に結果全体が必要になりますが、`ArrowStream` は到着したレコードバッチの数列として配信されるため、コンシューマーはそれらを到着に合わせてインクリメンタルに読み取れます。これにより、データセット全体を先にマテリアライズすることなく、クエリ結果をそのまま可視化ツールや分析ツールへストリーミングするのに適しています。

結果をストリーミングするには、ClickHouse の HTTP インターフェイスに `POST` リクエストでクエリを送信し、レスポンスを Arrow ストリームとして読み取ります。Arrow 出力の圧縮は
[`output_format_arrow_compression_method`](/ja/operations/settings/formats#output_format_arrow_compression_method)
設定で無効にし、コンシューマーが受信したバッチをその場で直接デコードできるようにします。

`ArrowStream` の出力は生のバイナリであるため、ターミナルに表示する代わりにコンシューマーへパイプします。このストリームは自己記述的で (独自のスキーマを持つため) 、ここではこれをそのまま
[`clickhouse-local`](/ja/operations/utilities/clickhouse-local)
にパイプします。`clickhouse-local` は `--input-format ArrowStream` で入力されたバッチを読み取り、それらをテーブルとしてクエリできます。`forex` テーブルは大きいため、この例を簡潔にするために、`WHERE`
述語と `LIMIT` でリモートクエリの範囲を絞ります。

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
        WHERE base = 'USD' AND quote = 'CHF'
        ORDER BY datetime ASC
        LIMIT 5
        FORMAT ArrowStream
        SETTINGS output_format_arrow_compression_method='none'" \
  | clickhouse-local --input-format ArrowStream \
      --query "SELECT * FROM table ORDER BY last_update ASC FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬────bid─┬────ask─┬────────────────spread─┐
1. │ USD.CHF    │ 2000-05-30 17:23:44.000 │  1.688 │ 1.6885 │ 0.0005000829696655273 │
2. │ USD.CHF    │ 2000-05-30 17:23:46.000 │ 1.6885 │  1.689 │ 0.0004999637603759766 │
3. │ USD.CHF    │ 2000-05-30 17:23:48.000 │ 1.6886 │ 1.6891 │ 0.0005000829696655273 │
4. │ USD.CHF    │ 2000-05-30 17:23:49.000 │ 1.6888 │ 1.6893 │ 0.0004999637603759766 │
5. │ USD.CHF    │ 2000-05-30 17:24:45.000 │  1.689 │ 1.6895 │ 0.0004999637603759766 │
   └────────────┴─────────────────────────┴────────┴────────┴───────────────────────┘
```

同じストリームは、Arrow 対応の任意のクライアントでインクリメンタルに利用でき、
結果全体をバッファリングするのではなく、バッチ単位で読み取れます。たとえば、
[Apache Arrow JavaScript library](https://arrow.apache.org/docs/js/) を使うと、
`RecordBatchReader` はサーバーからストリーミングされた各レコードバッチを
受信し次第返します：

```js
const reader = await RecordBatchReader.from(response);
await reader.open();
for await (const recordBatch of reader) {
    const batchTable = new Table(recordBatch);
    const ipcStream = tableToIPC(batchTable, 'stream');
    const bytes = new Uint8Array(ipcStream);
    table.update(bytes);
}
```

ClickHouse から `ArrowStream` データを [Perspective](https://perspective.finos.org/) に
ストリーミングしてリアルタイムで可視化する一連の手順については、
ブログ記事
[ClickHouse、Apache Arrow、Perspective を使ったリアルタイム可視化のストリーミング](https://clickhouse.com/blog/streaming-real-time-visualizations-clickhouse-apache-arrow-perpsective)
を参照してください。

<div id="format-settings">
  ## フォーマット設定
</div>

`ArrowStream` は [`Arrow`](/ja/interfaces/formats/Arrow) フォーマットと同じフォーマット設定を使用します。

| 設定                                                                           | 説明                                                                                                    | デフォルト       |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ----------- |
| `input_format_arrow_allow_missing_columns`                                   | Arrow 入力フォーマットの読み取り時に、欠落しているカラムを許可します。                                                                | `1`         |
| `input_format_arrow_case_insensitive_column_matching`                        | Arrow のカラムと CH columns の照合時に、大文字と小文字を区別しません。                                                          | `0`         |
| `input_format_arrow_import_nested`                                           | 廃止された設定で、効果はありません。                                                                                    | `0`         |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference` | Arrow フォーマットのスキーマ推論時に、サポートされていない型のカラムをスキップします。                                                        | `0`         |
| `output_format_arrow_compression_method`                                     | Arrow 出力フォーマットの圧縮方式。サポートされる codec: lz4&#95;frame、zstd、none (非圧縮)                                      | `lz4_frame` |
| `output_format_arrow_date_as_uint16`                                         | `Date` の値を 32 ビットの Arrow DATE32 型 (読み戻し時は Date32) に変換する代わりに、プレーンな 16 ビット数値 (読み戻し時は UInt16) として書き込みます。 | `0`         |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                       | FixedString 型のカラムでは、Binary の代わりに Arrow FIXED&#95;SIZE&#95;BINARY 型を使用します。                             | `1`         |
| `output_format_arrow_low_cardinality_as_dictionary`                          | LowCardinality 型を Dictionary Arrow 型として出力できるようにします。                                                   | `0`         |
| `output_format_arrow_string_as_string`                                       | String 型のカラムでは、Binary の代わりに Arrow String 型を使用します。                                                     | `1`         |
| `output_format_arrow_unsupported_types_as_binary`                            | 変換できない型を生のバイナリデータとして出力します。false の場合、そのような型では UNKNOWN&#95;TYPE 例外が発生します。                               | `1`         |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                      | Arrow フォーマットの dictionary indexes には常に 64 ビット整数を使用します。                                                 | `0`         |
| `output_format_arrow_use_signed_indexes_for_dictionary`                      | Arrow フォーマットの dictionary indexes には符号付き整数を使用します。                                                      | `1`         |