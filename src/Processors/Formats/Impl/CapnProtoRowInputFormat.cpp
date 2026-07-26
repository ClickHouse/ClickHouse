#include <Processors/Formats/Impl/CapnProtoRowInputFormat.h>
#if USE_CAPNP

#include <IO/ReadBuffer.h>
#include <Interpreters/Context.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSchemaInfo.h>
#include <capnp/serialize.h>
#include <capnp/dynamic.h>
#include <capnp/common.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

CapnProtoRowInputFormat::CapnProtoRowInputFormat(ReadBuffer & in_, SharedHeader header_, Params params_, const CapnProtoSchemaInfo & info, const FormatSettings & format_settings)
    : IRowInputFormat(std::move(header_), in_, std::move(params_))
    , parser(std::make_shared<CapnProtoSchemaParser>())
    , max_message_size(format_settings.capn_proto.max_message_size)
{
    // Parse the schema and fetch the root object
    schema = parser->getMessageSchema(info.getSchemaInfo());
    const auto & header = getPort().getHeader();
    serializer = std::make_unique<CapnProtoSerializer>(header.getDataTypes(), header.getNames(), schema, format_settings.capn_proto);
}

std::pair<kj::Array<capnp::word>, size_t> CapnProtoRowInputFormat::readMessagePrefix()
{
    uint32_t segment_count = 0;
    in->readStrict(reinterpret_cast<char*>(&segment_count), sizeof(uint32_t));
    /// Don't allow large amount of segments as it's done in capnproto library:
    /// https://github.com/capnproto/capnproto/blob/931074914eda9ca574b5c24d1169c0f7a5156594/c%2B%2B/src/capnp/serialize.c%2B%2B#L181
    /// Large amount of segments can indicate that corruption happened.
    if (segment_count >= 512)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Message has too many segments. Most likely, data was corrupted");

    // one for segmentCount and one because segmentCount starts from 0
    const auto prefix_size = (2 + segment_count) * sizeof(uint32_t);
    const auto words_prefix_size = (segment_count + 1) / 2 + 1;
    auto prefix = kj::heapArray<capnp::word>(words_prefix_size);
    auto prefix_chars = prefix.asChars();
    ::memcpy(prefix_chars.begin(), &segment_count, sizeof(uint32_t));

    // read size of each segment
    for (size_t i = 0; i <= segment_count; ++i)
        in->readStrict(prefix_chars.begin() + ((i + 1) * sizeof(uint32_t)), sizeof(uint32_t));

    return {std::move(prefix), prefix_size};
}

kj::Array<capnp::word> CapnProtoRowInputFormat::readMessage()
{
    auto [prefix, prefix_size] = readMessagePrefix();
    auto prefix_chars = prefix.asChars();

    // calculate size of message
    const auto expected_words = capnp::expectedSizeInWordsFromPrefix(prefix);
    const auto expected_bytes = expected_words * sizeof(capnp::word);

    if (expected_bytes > max_message_size)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "CapnProto message size {} exceeds maximum allowed size {}. Most likely, data is corrupted or format mismatch occurred",
            expected_bytes, max_message_size);

    const auto data_size = expected_bytes - prefix_size;
    auto msg = kj::heapArray<capnp::word>(expected_words);
    auto msg_chars = msg.asChars();

    // read full message
    ::memcpy(msg_chars.begin(), prefix_chars.begin(), prefix_size);
    in->readStrict(msg_chars.begin() + prefix_size, data_size);

    return msg;
}

void CapnProtoRowInputFormat::skipMessage()
{
    auto [prefix, prefix_size] = readMessagePrefix();

    // calculate size of message
    const auto expected_bytes = capnp::expectedSizeInWordsFromPrefix(prefix) * sizeof(capnp::word);
    const auto data_size = expected_bytes - prefix_size;

    // skip full message
    in->ignore(data_size);
}

bool CapnProtoRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    try
    {
        auto array = readMessage();
        capnp::FlatArrayMessageReader msg(array);
        auto root_reader = msg.getRoot<capnp::DynamicStruct>(schema);
        serializer->readRow(columns, root_reader);
    }
    catch (const kj::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read row: {}", e.getDescription().cStr());
    }

    return true;
}

size_t CapnProtoRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        skipMessage();
        ++num_rows;
    }

    return num_rows;
}

CapnProtoSchemaReader::CapnProtoSchemaReader(const FormatSettings & format_settings_) : format_settings(format_settings_)
{
}

NamesAndTypesList CapnProtoSchemaReader::readSchema()
{
    auto schema_info = FormatSchemaInfo(
        /*format_schema_source=*/format_settings.schema.format_schema_source,
        /*format_schema=*/format_settings.schema.format_schema,
        /*format_schema_message_name=*/format_settings.schema.format_schema_message_name,
        /*format=*/"CapnProto",
        /*require_message=*/true,
        /*is_server=*/format_settings.schema.is_server,
        /*format_schema_path=*/format_settings.schema.format_schema_path);

    auto schema_parser = CapnProtoSchemaParser();
    auto schema = schema_parser.getMessageSchema(schema_info);
    return capnProtoSchemaToCHSchema(schema, format_settings.capn_proto.skip_fields_with_unsupported_types_in_schema_inference);
}

void registerInputFormatCapnProto(FormatFactory & factory);
void registerInputFormatCapnProto(FormatFactory & factory)
{
    factory.registerInputFormat(
        "CapnProto",
        [](ReadBuffer & buf, const Block & sample, IRowInputFormat::Params params, const FormatSettings & settings)
        {
            return std::make_shared<CapnProtoRowInputFormat>(
                buf,
                std::make_shared<const Block>(sample),
                std::move(params),
                CapnProtoSchemaInfo(settings, "CapnProto", sample, settings.capn_proto.use_autogenerated_schema),
                settings);
        });
    factory.markFormatSupportsSubsetOfColumns("CapnProto");
    factory.registerFileExtension("capnp", "CapnProto");
    factory.registerAdditionalInfoForSchemaCacheGetter(
        "CapnProto",
        [](const FormatSettings & settings)
        {
            return fmt::format(
                "format_schema={}, skip_fields_with_unsupported_types_in_schema_inference={}",
                settings.schema.format_schema,
                settings.capn_proto.skip_fields_with_unsupported_types_in_schema_inference);
        });

    factory.setDocumentation("CapnProto", Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

<CloudNotSupportedBadge/>

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `CapnProto` format is a binary message format similar to the [`Protocol Buffers`](https://developers.google.com/protocol-buffers/) format and [Thrift](https://en.wikipedia.org/wiki/Apache_Thrift), but not like [JSON](/reference/formats/JSON/JSON) or [MessagePack](https://msgpack.org/).
CapnProto messages are strictly typed and not self-describing, meaning they need an external schema description. The schema is applied on the fly and cached for each query.

See also [Format Schema](/reference/formats/index#formatschema).

## Data types matching {#data_types-matching-capnproto}

The table below shows supported data types and how they match ClickHouse [data types](/reference/data-types/index) in `INSERT` and `SELECT` queries.

| CapnProto data type (`INSERT`)                       | ClickHouse data type                                                                                                                                                           | CapnProto data type (`SELECT`)                       |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|
| `UINT8`, `BOOL`                                      | [UInt8](/reference/data-types/int-uint)                                                                                                                         | `UINT8`                                              |
| `INT8`                                               | [Int8](/reference/data-types/int-uint)                                                                                                                          | `INT8`                                               |
| `UINT16`                                             | [UInt16](/reference/data-types/int-uint), [Date](/reference/data-types/date)                                                                     | `UINT16`                                             |
| `INT16`                                              | [Int16](/reference/data-types/int-uint)                                                                                                                         | `INT16`                                              |
| `UINT32`                                             | [UInt32](/reference/data-types/int-uint), [DateTime](/reference/data-types/datetime)                                                             | `UINT32`                                             |
| `INT32`                                              | [Int32](/reference/data-types/int-uint), [Decimal32](/reference/data-types/decimal)                                                              | `INT32`                                              |
| `UINT64`                                             | [UInt64](/reference/data-types/int-uint)                                                                                                                        | `UINT64`                                             |
| `INT64`                                              | [Int64](/reference/data-types/int-uint), [DateTime64](/reference/data-types/datetime), [Decimal64](/reference/data-types/decimal) | `INT64`                                              |
| `FLOAT32`                                            | [Float32](/reference/data-types/float)                                                                                                                          | `FLOAT32`                                            |
| `FLOAT64`                                            | [Float64](/reference/data-types/float)                                                                                                                          | `FLOAT64`                                            |
| `TEXT, DATA`                                         | [String](/reference/data-types/string), [FixedString](/reference/data-types/fixedstring)                                                         | `TEXT, DATA`                                         |
| `union(T, Void), union(Void, T)`                     | [Nullable(T)](/reference/data-types/date)                                                                                                                       | `union(T, Void), union(Void, T)`                     |
| `ENUM`                                               | [Enum(8/16)](/reference/data-types/enum)                                                                                                                        | `ENUM`                                               |
| `LIST`                                               | [Array](/reference/data-types/array)                                                                                                                            | `LIST`                                               |
| `STRUCT`                                             | [Tuple](/reference/data-types/tuple)                                                                                                                            | `STRUCT`                                             |
| `UINT32`                                             | [IPv4](/reference/data-types/ipv4)                                                                                                                              | `UINT32`                                             |
| `DATA`                                               | [IPv6](/reference/data-types/ipv6)                                                                                                                              | `DATA`                                               |
| `DATA`                                               | [Int128/UInt128/Int256/UInt256](/reference/data-types/int-uint)                                                                                                 | `DATA`                                               |
| `DATA`                                               | [Decimal128/Decimal256](/reference/data-types/decimal)                                                                                                          | `DATA`                                               |
| `STRUCT(entries LIST(STRUCT(key Key, value Value)))` | [Map](/reference/data-types/map)                                                                                                                                | `STRUCT(entries LIST(STRUCT(key Key, value Value)))` |

- Integer types can be converted into each other during input/output.
- For working with `Enum` in CapnProto format use the [format_capn_proto_enum_comparising_mode](/reference/settings/formats#format_capn_proto_enum_comparising_mode) setting.
- Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

## Example usage {#example-usage}

### Inserting and selecting data {#inserting-and-selecting-data-capnproto}

You can insert CapnProto data from a file into ClickHouse table by the following command:

```bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_schema = 'schema:Message' FORMAT CapnProto"
```

Where the `schema.capnp` looks like this:

```capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

You can select data from a ClickHouse table and save them into some file in the `CapnProto` format using the following command:

```bash
$ clickhouse-client --query = "SELECT * FROM test.hits FORMAT CapnProto SETTINGS format_schema = 'schema:Message'"
```

### Using autogenerated schema {#using-autogenerated-capn-proto-schema}

If you don't have an external `CapnProto` schema for your data, you can still output/input data in `CapnProto` format using autogenerated schema.

For example:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS format_capn_proto_use_autogenerated_schema=1
```

In this case, ClickHouse will autogenerate CapnProto schema according to the table structure using function [structureToCapnProtoSchema](/reference/functions/regular-functions/other-functions#structureToCapnProtoSchema) and will use this schema to serialize data in CapnProto format.

You can also read CapnProto file with autogenerated schema (in this case the file must be created using the same schema):

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_capn_proto_use_autogenerated_schema=1 FORMAT CapnProto"
```

## Format settings {#format-settings}

The setting [`format_capn_proto_use_autogenerated_schema`](/reference/settings/formats#format_capn_proto_use_autogenerated_schema) is enabled by default and is applicable if [`format_schema`](/reference/formats/index#formatschema) is not set.

You can also save the autogenerated schema to a file during input/output using setting [`output_format_schema`](/reference/settings/formats#output_format_schema). 

For example:

```sql
SELECT * FROM test.hits 
FORMAT CapnProto 
SETTINGS 
    format_capn_proto_use_autogenerated_schema=1,
    output_format_schema='path/to/schema/schema.capnp'
```
In this case, the autogenerated `CapnProto` schema will be saved in file `path/to/schema/schema.capnp`.
)DOCS_MD"});
}

void registerCapnProtoSchemaReader(FormatFactory & factory);
void registerCapnProtoSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("CapnProto", [](const FormatSettings & settings)
    {
       return std::make_shared<CapnProtoSchemaReader>(settings);
    });
}

}

#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatCapnProto(FormatFactory &);
    void registerCapnProtoSchemaReader(FormatFactory &);
    void registerInputFormatCapnProto(FormatFactory &) {}
    void registerCapnProtoSchemaReader(FormatFactory &) {}
}

#endif // USE_CAPNP
