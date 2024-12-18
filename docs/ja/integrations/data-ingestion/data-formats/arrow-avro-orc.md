---
sidebar_label: Avro、Arrow、および ORC
sidebar_position: 5
slug: /ja/integrations/data-formats/arrow-avro-orc
---

# ClickHouseでのAvro、Arrow、およびORCデータの操作

Apacheは、分析環境で広く使用される複数のデータフォーマットをリリースしており、その中には人気のある[Avro](https://avro.apache.org/)、[Arrow](https://arrow.apache.org/)、そして[Orc](https://orc.apache.org/)があります。ClickHouseはリストから選択されたデータをインポートおよびエクスポートすることをサポートしています。

## Avroフォーマットでのインポートとエクスポート

ClickHouseは、Hadoopシステムで広く使われている[Apache Avro](https://avro.apache.org/)データファイルの読み取りと書き込みをサポートしています。

[Avroファイル](assets/data.avro)からインポートするには、`INSERT`文で[Avro](/docs/ja/interfaces/formats.md/#data-format-avro)フォーマットを使用します：

```sql
INSERT INTO sometable
FROM INFILE 'data.avro'
FORMAT Avro
```

[file()](/docs/ja/sql-reference/functions/files.md/#file)関数を使用して、実際にデータをインポートする前にAvroファイルを調査することも可能です：

```sql
SELECT path, hits
FROM file('data.avro', Avro)
ORDER BY hits DESC
LIMIT 5;
```
```response
┌─path────────────┬──hits─┐
│ Amy_Poehler     │ 62732 │
│ Adam_Goldberg   │ 42338 │
│ Aaron_Spelling  │ 25128 │
│ Absence_seizure │ 18152 │
│ Ammon_Bundy     │ 11890 │
└─────────────────┴───────┘
```

Avroファイルへのエクスポート：

```sql
SELECT * FROM sometable
INTO OUTFILE 'export.avro'
FORMAT Avro;
```

### AvroとClickHouseデータタイプ

Avroファイルをインポートまたはエクスポートする際は、[データタイプのマッチング](/docs/ja/interfaces/formats.md/#data_types-matching)を考慮に入れてください。Avroファイルからデータをロードする際には、明示的な型キャストを使用して変換してください：

```sql
SELECT
    date,
    toDate(date)
FROM file('data.avro', Avro)
LIMIT 3;
```
```response
┌──date─┬─toDate(date)─┐
│ 16556 │   2015-05-01 │
│ 16556 │   2015-05-01 │
│ 16556 │   2015-05-01 │
└───────┴──────────────┘
```

### KafkaにおけるAvroメッセージ

KafkaメッセージがAvroフォーマットを使用している場合、ClickHouseは[AvroConfluent](/docs/ja/interfaces/formats.md/#data-format-avro-confluent)フォーマットと[Kafka](/docs/ja/engines/table-engines/integrations/kafka.md)エンジンを使用してこのようなストリームを読み取ることができます：

```sql
CREATE TABLE some_topic_stream
(
    field1 UInt32,
    field2 String
)
ENGINE = Kafka() SETTINGS
kafka_broker_list = 'localhost',
kafka_topic_list = 'some_topic',
kafka_group_name = 'some_group',
kafka_format = 'AvroConfluent';
```

## Arrowフォーマットの操作

もう一つのカラム形式のフォーマットは[Apache Arrow](https://arrow.apache.org/)で、ClickHouseでもインポートとエクスポートをサポートしています。[Arrowファイル](assets/data.arrow)からデータをインポートするには、[Arrow](/docs/ja/interfaces/formats.md/#data-format-arrow)フォーマットを使用します：

```sql
INSERT INTO sometable
FROM INFILE 'data.arrow'
FORMAT Arrow
```

Arrowファイルへのエクスポートも同様の方法で行います：

```sql
SELECT * FROM sometable
INTO OUTFILE 'export.arrow'
FORMAT Arrow
```

また、[データタイプのマッチング](/docs/ja/interfaces/formats.md/#data-types-matching-arrow)を確認し、手動で変換が必要な場合があります。

### Arrowデータストリーミング

[ArrowStream](/docs/ja/interfaces/formats.md/#data-format-arrow-stream)フォーマットはArrowストリーミング（メモリ内処理用）で使用できます。ClickHouseはArrowストリームの読み書きが可能です。

ClickHouseがArrowデータをストリームする方法を示すために、次のPythonスクリプトにパイプでデータを渡します（これはArrowストリーミングフォーマットで入力ストリームを読み取り、結果をPandasテーブルとして出力します）：

```python
import sys, pyarrow as pa

with pa.ipc.open_stream(sys.stdin.buffer) as reader:
  print(reader.read_pandas())
```

次に、そのスクリプトに出力をパイプさせることでClickHouseからデータをストリームします：

```bash
clickhouse-client -q "SELECT path, hits FROM some_data LIMIT 3 FORMAT ArrowStream" | python3 arrow.py
```
```response
                           path  hits
0       b'Akiba_Hebrew_Academy'   241
1           b'Aegithina_tiphia'    34
2  b'1971-72_Utah_Stars_season'     1
```

ClickHouseは同じArrowStreamフォーマットを使用してArrowストリームを読み取ることもできます：

```sql
arrow-stream | clickhouse-client -q "INSERT INTO sometable FORMAT ArrowStream"
```

`arrow-stream`をArrowストリーミングデータの可能なソースとして使用しました。

## ORCデータのインポートとエクスポート

[Apache ORC](https://orc.apache.org/)フォーマットは通常Hadoopで使用されるカラム形式のストレージフォーマットです。ClickHouseは[ORCフォーマット](/docs/ja/interfaces/formats.md/#data-format-orc)を使用した[Orcデータ](assets/data.orc)のインポートとエクスポートをサポートしています：

```sql
SELECT *
FROM sometable
INTO OUTFILE 'data.orc'
FORMAT ORC;

INSERT INTO sometable
FROM INFILE 'data.orc'
FORMAT ORC;
```

エクスポートとインポートを調整するために、[データタイプのマッチング](/docs/ja/interfaces/formats.md/#data-types-matching-orc)および[追加設定](/docs/ja/interfaces/formats.md/#parquet-format-settings)も確認してください。

## さらなる学習

ClickHouseは、さまざまなシナリオとプラットフォームに対応するために多くのフォーマット（テキストおよびバイナリ）をサポートしています。以下の記事で他のフォーマットとその活用方法をさらに探索してください：

- [CSVとTSVフォーマット](csv-tsv.md)
- [JSONフォーマット](/docs/ja/integrations/data-ingestion/data-formats/json/intro.md)
- [正規表現とテンプレート](templates-regex.md)
- [ネイティブとバイナリフォーマット](binary.md)
- [SQLフォーマット](sql.md)

また、[clickhouse-local](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)も確認してください - Clickhouseサーバーを必要とせずにローカル/リモートファイルで作業するためのポータブルでフル機能のツールです。
