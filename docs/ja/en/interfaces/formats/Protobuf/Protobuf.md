---
alias: []
description: 'Protobufフォーマットに関するドキュメント'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 説明
</div>

Protobuf 形式は、[Protocol Buffers](https://protobuf.dev/) のフォーマットです。

このフォーマットでは外部のフォーマットスキーマが必要であり、クエリ間で cache されます。

ClickHouse は以下をサポートしています。

* `proto2` と `proto3` の両方の構文。
* `Repeated`/`optional`/`required` フィールド。

テーブルのカラムと Protocol Buffers&#39; メッセージ型のフィールドの対応関係を判断するために、ClickHouse はそれらの名前を比較します。
この比較では大文字と小文字を区別せず、`_` (アンダースコア) と `.` (ドット) は同じものとして扱われます。
カラムと Protocol Buffers&#39; メッセージのフィールドの型が異なる場合は、必要な変換が適用されます。

ネストされたメッセージもサポートされています。たとえば、次のメッセージ型のフィールド `z` では:

```capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse は、`x.y.z` (または `x_y_z`、`X.y_Z` など) という名前のカラムを見つけようとします。

ネストされたメッセージは、[ネストされたデータ構造](/ja/sql-reference/data-types/nested-data-structures/index.md) の入力または出力に適しています。

以下のような Protobuf スキーマで定義されたデフォルト値は適用されず、代わりに [テーブルのデフォルト値](/ja/sql-reference/statements/create/table#default_values) が使用されます。

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

メッセージに [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) が含まれ、`input_format_protobuf_oneof_presence` が設定されている場合、ClickHouse は oneof のどのフィールドが見つかったかを示すカラムを埋めます。

```capnp
syntax = "proto3";

message StringOrString {
  oneof string_oneof {
    string string1 = 1;
    string string2 = 42;
  }
}
```

```sql
CREATE TABLE string_or_string ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 42))  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string
```

```text
   ┌─────────┬─────────┬──────────────┐
   │ string1 │ string2 │ string_oneof │
   ├─────────┼─────────┼──────────────┤
1. │         │ string2 │ world        │
   ├─────────┼─────────┼──────────────┤
2. │ string1 │         │ hello        │
   └─────────┴─────────┴──────────────┘
```

存在を示すカラム名は、oneof の名前と同じである必要があります。
ネストされたメッセージもサポートされています ([basic-examples](#basic-examples) を参照) 。空のメッセージもサポートされています。
使用できる型は Int8、UInt8、Int16、UInt16、Int32、UInt32、Int64、UInt64、Enum、Enum8、Enum16 です。
Enum (Enum8 および Enum16 を含む) には、oneof の取り得るすべてのタグに加えて、不在を示す 0 を含める必要があります。文字列表現は問いません。

設定 [`input_format_protobuf_oneof_presence`](/ja/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) は、デフォルトで無効です。

ClickHouse は protobuf メッセージを 長さ区切りフォーマットで入出力します。
これは、各メッセージの前にその長さを [可変長整数 (varint) ](https://developers.google.com/protocol-buffers/docs/encoding#varints) として書き込む必要があることを意味します。

<div id="example-usage">
  ## 使用例
</div>

<div id="basic-examples">
  ### データの読み書き
</div>

:::note 例のファイル
この例で使用するファイルは、[examples repository](https://github.com/ClickHouse/formats/ProtoBuf) で入手できます。
:::

この例では、ファイル `protobuf_message.bin` からデータを ClickHouse テーブルに読み込みます。続いて、`Protobuf` 形式を使用して、それを `protobuf_message_from_clickhouse.bin` というファイルに書き出します。

`schemafile.proto` ファイルの内容が次のとおりであるとします。

```capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

<details>
  <summary>バイナリファイルの生成</summary>

  Protobuf 形式でデータをシリアライズおよびデシリアライズする方法をすでに理解している場合は、この手順は省略できます。

  ここでは、Python を使ってデータを `protobuf_message.bin` にシリアライズし、それを ClickHouse に読み込みます。
  別の言語を使いたい場合は、[&quot;一般的な言語で長さ区切りの Protobuf メッセージを読み書きする方法&quot;](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages)も参照してください。

  次のコマンドを実行して、`schemafile.proto` と
  同じディレクトリに `schemafile_pb2.py` という名前の Python ファイルを生成します。このファイルには、
  `UserData` Protobuf メッセージを表す Python クラスが含まれています。

  ```bash
  protoc --python_out=. schemafile.proto
  ```

  次に、`schemafile_pb2.py` と同じ
  ディレクトリに `generate_protobuf_data.py` という名前の新しい Python ファイルを作成します。以下のコードを貼り付けてください。

  ```python
  import schemafile_pb2  # 'protoc' によって生成されたモジュール
  from google.protobuf import text_format
  from google.protobuf.internal.encoder import _VarintBytes # 内部 varint エンコーダをインポート

  def create_user_data_message(name, surname, birthDate, phoneNumbers):
      """
      UserData Protobuf メッセージを作成して値を設定します。
      """
      message = schemafile_pb2.MessageType()
      message.name = name
      message.surname = surname
      message.birthDate = birthDate
      message.phoneNumbers.extend(phoneNumbers)
      return message

  # この例で使用するユーザーデータ
  data_to_serialize = [
      {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
      {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
      {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
  ]

  output_filename = "protobuf_messages.bin"

  # バイナリファイルをバイナリ書き込みモード ('wb') で開く
  with open(output_filename, "wb") as f:
      for item in data_to_serialize:
          # 現在のユーザーの Protobuf メッセージインスタンスを作成
          message = create_user_data_message(
              item["name"],
              item["surname"],
              item["birthDate"],
              item["phoneNumbers"]
          )

          # メッセージをシリアライズ
          serialized_data = message.SerializeToString()

          # シリアライズされたデータの長さを取得
          message_length = len(serialized_data)

          # Protobuf ライブラリの内部 _VarintBytes を使って長さをエンコード
          length_prefix = _VarintBytes(message_length)

          # 長さプレフィックスを書き込む
          f.write(length_prefix)
          # シリアライズされたメッセージデータを書き込む
          f.write(serialized_data)

  print(f"Protobuf messages (length-delimited) written to {output_filename}")

  # --- 任意: 検証 (読み戻して表示) ---
  # 読み戻しには、varint 用の内部 Protobuf デコーダも使用します。
  from google.protobuf.internal.decoder import _DecodeVarint32

  print("\n--- 読み戻しによる検証 ---")
  with open(output_filename, "rb") as f:
      buf = f.read() # varint のデコードを簡単にするため、ファイル全体をバッファに読み込む
      n = 0
      while n < len(buf):
          # varint の長さプレフィックスをデコード
          msg_len, new_pos = _DecodeVarint32(buf, n)
          n = new_pos

          # メッセージデータを取り出す
          message_data = buf[n:n+msg_len]
          n += msg_len

          # メッセージをパース
          decoded_message = schemafile_pb2.MessageType()
          decoded_message.ParseFromString(message_data)
          print(text_format.MessageToString(decoded_message, as_utf8=True))
  ```

  次に、コマンドラインからスクリプトを実行します。たとえば `uv` を使って、
  Python 仮想環境で実行することをおすすめします。

  ```bash
  uv venv proto-venv
  source proto-venv/bin/activate
  ```

  次の Python ライブラリをインストールする必要があります。

  ```bash
  uv pip install --upgrade protobuf
  ```

  スクリプトを実行してバイナリファイルを生成します。

  ```bash
  python generate_protobuf_data.py
  ```
</details>

スキーマに一致する ClickHouseテーブルを作成します。

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

コマンドラインからテーブルにデータを挿入します:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

また、Protobuf 形式を使用して、データをバイナリファイルへ書き戻すこともできます:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

お使いのProtobufスキーマを使って、ClickHouseからファイル`protobuf_message_from_clickhouse.bin`に書き出されたデータをデシリアライズできるようになりました。

<div id="basic-examples-cloud">
  ### ClickHouse Cloud を使用したデータの読み取りと書き込み
</div>

ClickHouse Cloud では、Protobuf スキーマファイルをアップロードできません。ただし、`format_protobuf_schema`
設定を使用して、クエリ内でスキーマを指定できます。この例では、ローカル
マシンからシリアライズされたデータを読み取り、ClickHouse Cloud のテーブルに挿入する方法を示します。

前の例と同様に、ClickHouse Cloud で Protobuf スキーマに従ってテーブルを作成します。

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

設定 `format_schema_source` は、設定 `format_schema` の参照元を定義します

設定可能な値:

* &#39;file&#39; (デフォルト) : Cloud ではサポートされていません
* &#39;string&#39;: `format_schema` はスキーマの内容そのものです。
* &#39;query&#39;: `format_schema` はスキーマを取得するためのクエリです。

<div id="format-schema-source-string">
  ### `format_schema_source='string'`
</div>

スキーマを文字列として指定してデータを ClickHouse Cloud に挿入するには、次を実行します。

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

テーブルに挿入したデータを取得します:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="format-schema-source-query">
  ### `format_schema_source='query'`
</div>

Protobuf スキーマはテーブルに保存することもできます。

データの挿入先となるテーブルを ClickHouse Cloud に作成します。

```sql
CREATE TABLE testing.protobuf_schema (
  schema String
)
ENGINE = MergeTree()
ORDER BY tuple();
```

```sql
INSERT INTO testing.protobuf_schema VALUES ('syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};');
```

実行するクエリでスキーマを指定して、データをClickHouse Cloudに挿入します:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

テーブルに挿入されたデータを選択します:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="using-autogenerated-protobuf-schema">
  ### 自動生成されたスキーマを使用する
</div>

データ用の外部 Protobuf スキーマがない場合でも、自動生成されたスキーマを使って、
Protobuf フォーマットでデータを出力/入力できます。この場合は、`format_protobuf_use_autogenerated_schema` 設定を使用します。

例:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

この場合、ClickHouse は関数
[`structureToProtobufSchema`](/ja/sql-reference/functions/other-functions#structureToProtobufSchema)を使用して、テーブル構造に基づいて Protobuf スキーマを自動生成します。次に、このスキーマを使用してデータを Protobuf フォーマットにシリアライズします。

自動生成されたスキーマを使用して Protobuf ファイルを読み込むこともできます。この場合、ファイルは同じスキーマを使用して作成されている必要があります。

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

設定 [`format_protobuf_use_autogenerated_schema`](/ja/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) はデフォルトで有効になっており、[`format_schema`](/ja/operations/settings/formats#format_schema) が設定されていない場合に適用されます。

また、入出力時に設定 [`output_format_schema`](/ja/operations/settings/formats#output_format_schema) を使用して、自動生成されたスキーマをファイルに保存することもできます。例えば:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

この場合、自動生成されたProtobufスキーマは `path/to/schema/schema.capnp` ファイルに保存されます。

<div id="drop-protobuf-cache">
  ### Protobuf cache を削除
</div>

[`format_schema_path`](/ja/operations/server-configuration-parameters/settings.md/#format_schema_path) から読み込まれた Protobuf スキーマを再読み込みするには、[`SYSTEM DROP ... FORMAT CACHE`](/ja/sql-reference/statements/system.md/#system-drop-schema-format) ステートメントを使用します。

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```