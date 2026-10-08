---
alias: []
description: 'Documentation for the Protobuf format'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `Protobuf` format is the [Protocol Buffers](https://protobuf.dev/) format.

This format requires an external format schema, which is cached between queries.

ClickHouse supports:
- both `proto2` and `proto3` syntaxes.
- `Repeated`/`optional`/`required` fields.

To find the correspondence between table columns and fields of the Protocol Buffers' message type, ClickHouse compares their names.
This comparison is case-insensitive and the characters `_` (underscore) and `.` (dot) are considered as equal.
If the types of a column and a field of the Protocol Buffers' message are different, then the necessary conversion is applied.

Nested messages are supported. For example, for the field `z` in the following message type:

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

ClickHouse tries to find a column named `x.y.z` (or `x_y_z` or `X.y_Z` and so on).

Nested messages are suitable for input or output of a [nested data structures](/sql-reference/data-types/nested-data-structures/index.md).

Default values defined in a protobuf schema like the one that follows are not applied, rather the [table defaults](/sql-reference/statements/create/table#default_values) are used instead of them:

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

If a message contains [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) and `input_format_protobuf_oneof_presence` is set, ClickHouse fills column that indicates which field of oneof was found.

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

Name of the column that indicates presence must be the same as the name of oneof. Nested messages are supported (see  [basic-examples](#basic-examples)).
Allowed types are Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Enum, Enum8 or Enum16.
Enum (as well as Enum8 or Enum16) must contain all oneof' possible tags plus 0 to indicate absence, string representations does not matter.

The setting [`input_format_protobuf_oneof_presence`](/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) is disabled by default

ClickHouse inputs and outputs protobuf messages in the `length-delimited` format.
This means that before every message its length should be written as a [variable width integer (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

## Example usage {#example-usage}

### Reading and writing data {#basic-examples}

:::note Example files
The files used in this example are available in the [examples repository](https://github.com/ClickHouse/formats/ProtoBuf)
:::

In this example we will read some data from a file `protobuf_message.bin` into a ClickHouse table. We'll then write it
back out to a file called `protobuf_message_from_clickhouse.bin` using the `Protobuf` format. 

Given the file `schemafile.proto`:

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
<summary>Generating the binary file</summary>
  
If you already know how to serialize and deserialize data in the `Protobuf` format, you can skip this step.

We'll use Python to serialize some data into `protobuf_message.bin` and read it into ClickHouse.
If there is another language you want to use, see also: ["How to read/write length-delimited Protobuf messages in popular languages"](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

Run the following command to generate a Python file named `schemafile_pb2.py` in 
the same directory as `schemafile.proto`. This file contains the Python classes 
that represent your `UserData` Protobuf message:

```bash
protoc --python_out=. schemafile.proto
```

Now, create a new Python file named `generate_protobuf_data.py`, in the same 
directory as `schemafile_pb2.py`. Paste the following code into it:

```python
import schemafile_pb2  # Module generated by 'protoc'
from google.protobuf import text_format
from google.protobuf.internal.encoder import _VarintBytes # Import the internal varint encoder

def create_user_data_message(name, surname, birthDate, phoneNumbers):
    """
    Creates and populates a UserData Protobuf message.
    """
    message = schemafile_pb2.MessageType()
    message.name = name
    message.surname = surname
    message.birthDate = birthDate
    message.phoneNumbers.extend(phoneNumbers)
    return message

# The data for our example users
data_to_serialize = [
    {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
    {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
    {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
]

output_filename = "protobuf_messages.bin"

# Open the binary file in write-binary mode ('wb')
with open(output_filename, "wb") as f:
    for item in data_to_serialize:
        # Create a Protobuf message instance for the current user
        message = create_user_data_message(
            item["name"],
            item["surname"],
            item["birthDate"],
            item["phoneNumbers"]
        )

        # Serialize the message
        serialized_data = message.SerializeToString()

        # Get the length of the serialized data
        message_length = len(serialized_data)

        # Use the Protobuf library's internal _VarintBytes to encode the length
        length_prefix = _VarintBytes(message_length)

        # Write the length prefix
        f.write(length_prefix)
        # Write the serialized message data
        f.write(serialized_data)

print(f"Protobuf messages (length-delimited) written to {output_filename}")

# --- Optional: Verification (reading back and printing) ---
# For reading back, we'll also use the internal Protobuf decoder for varints.
from google.protobuf.internal.decoder import _DecodeVarint32

print("\n--- Verifying by reading back ---")
with open(output_filename, "rb") as f:
    buf = f.read() # Read the whole file into a buffer for easier varint decoding
    n = 0
    while n < len(buf):
        # Decode the varint length prefix
        msg_len, new_pos = _DecodeVarint32(buf, n)
        n = new_pos
        
        # Extract the message data
        message_data = buf[n:n+msg_len]
        n += msg_len

        # Parse the message
        decoded_message = schemafile_pb2.MessageType()
        decoded_message.ParseFromString(message_data)
        print(text_format.MessageToString(decoded_message, as_utf8=True))
```

Now run the script from the command line. It is recommended to run it from a 
python virtual environment, for example using `uv`:

```bash
uv venv proto-venv
source proto-venv/bin/activate
```

You will need to install the following python libraries:

```bash
uv pip install --upgrade protobuf
```

Run the script to generate the binary file:

```bash
python generate_protobuf_data.py
```

</details>

Create a ClickHouse table matching the schema:

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

Insert the data into the table from the command line:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

You can also write the data back to a binary file using the `Protobuf` format:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

With your Protobuf schema, you can now deserialize the data which was written out from ClickHouse to file `protobuf_message_from_clickhouse.bin`.

### Reading and writing data using ClickHouse Cloud {#basic-examples-cloud}

With ClickHouse Cloud you are not able to upload a Protobuf schema file. However, you can use the `format_protobuf_schema` 
setting to specify the schema in the query. In this example, we show you how to read serialized data from your local
machine and insert it into a table in ClickHouse Cloud.

As in the previous example, create the table according to the schema of your Protobuf schema in ClickHouse Cloud:

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

The setting `format_schema_source` defines the source of setting `format_schema`

Possible values:
- 'file' (default): unsupported in Cloud
- 'string': The `format_schema` is the literal content of the schema.
- 'query': The `format_schema` is a query to retrieve the schema.

### `format_schema_source='string'` {#format-schema-source-string}

Insert the data into ClickHouse Cloud, specifying the schema as a string, run:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Select the data inserted into the table:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

### `format_schema_source='query'` {#format-schema-source-query}

You can also store your Protobuf schema in a table.

Create a table on ClickHouse Cloud to insert data into:

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

Insert the data into ClickHouse Cloud, specifying the schema as a query to run:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Select the data inserted into the table:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

### Using autogenerated schema {#using-autogenerated-protobuf-schema}

If you don't have an external Protobuf schema for your data, you can still output/input data in the Protobuf format 
using an autogenerated schema. For this use the `format_protobuf_use_autogenerated_schema` setting.

For example:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

In this case, ClickHouse will autogenerate the Protobuf schema according to the table structure using function 
[`structureToProtobufSchema`](/sql-reference/functions/other-functions#structureToProtobufSchema). It will then use this schema to serialize data in the Protobuf format.

You can also read a Protobuf file with the autogenerated schema. In this case it is necessary for the file to be created using the same schema:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

The setting [`format_protobuf_use_autogenerated_schema`](/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) is enabled by default and applies if [`format_schema`](/operations/settings/formats#format_schema) is not set.

You can also save autogenerated schema in the file during input/output using setting [`output_format_schema`](/operations/settings/formats#output_format_schema). For example:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

In this case autogenerated Protobuf schema will be saved in file `path/to/schema/schema.capnp`.

### Drop protobuf cache {#drop-protobuf-cache}

To reload the Protobuf schema loaded from [`format_schema_path`](/operations/server-configuration-parameters/settings.md/#format_schema_path) use the [`SYSTEM DROP ... FORMAT CACHE`](/sql-reference/statements/system.md/#system-drop-schema-format) statement.

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```
