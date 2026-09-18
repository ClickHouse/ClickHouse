#include <Columns/ColumnConst.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/JSONAsStringRowInputFormat.h>
#include <Formats/JSONUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

JSONAsRowInputFormat::JSONAsRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_) :
    JSONEachRowRowInputFormat(in_, header_, std::move(params_), format_settings_, false)
{
    if (header_->columns() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String or Object, but the number of columns is {}",
            header_->columns());
}

bool JSONAsRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    chassert(columns.size() == 1);
    chassert(serializations.size() == 1);

    if (!allow_new_rows)
        return false;

    skipWhitespaceIfAny(*in);
    if (!in->eof())
    {
        if (!data_in_square_brackets && *in->position() == ';')
        {
            /// ';' means the end of query, but it cannot be before ']'.
            return allow_new_rows = false;
        }
        if (data_in_square_brackets && *in->position() == ']')
        {
            /// ']' means the end of query.
            return allow_new_rows = false;
        }
    }

    if (!in->eof())
        readJSONObject(*columns[0]);

    skipWhitespaceIfAny(*in);
    if (!in->eof() && *in->position() == ',')
        ++in->position();
    skipWhitespaceIfAny(*in);

    return !in->eof();
}

JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, IRowInputFormat::Params params_, const FormatSettings & format_settings_)
    : JSONAsStringRowInputFormat(header_, std::make_unique<PeekableReadBuffer>(in_), params_, format_settings_)
{
}

JSONAsStringRowInputFormat::JSONAsStringRowInputFormat(
    SharedHeader header_, std::unique_ptr<PeekableReadBuffer> buf_, Params params_, const FormatSettings & format_settings_)
    : JSONAsRowInputFormat(header_, *buf_, params_, format_settings_), buf(std::move(buf_))
{
    if (!isString(removeNullable(removeLowCardinality(header_->getByPosition(0).type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the column type is {}",
            header_->getByPosition(0).type->getName());
}

void JSONAsStringRowInputFormat::setReadBuffer(ReadBuffer & in_)
{
    buf = std::make_unique<PeekableReadBuffer>(in_);
    JSONAsRowInputFormat::setReadBuffer(*buf);
}

void JSONAsStringRowInputFormat::resetReadBuffer()
{
    buf.reset();
    JSONAsRowInputFormat::resetReadBuffer();
}

void JSONAsStringRowInputFormat::readJSONObject(IColumn & column)
{
    PeekableReadBufferCheckpoint checkpoint{*buf};
    size_t balance = 0;
    bool quotes = false;

    if (*buf->position() != '{')
        throw Exception(ErrorCodes::INCORRECT_DATA, "JSON object must begin with '{{'.");

    ++buf->position();
    ++balance;

    char * pos = nullptr;

    while (balance)
    {
        if (buf->eof())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected end of file while parsing JSON object.");

        if (quotes)
        {
            pos = find_first_symbols<'"', '\\'>(buf->position(), buf->buffer().end());
            buf->position() = pos;
            if (buf->position() == buf->buffer().end())
                continue;
            if (*buf->position() == '"')
            {
                quotes = false;
                ++buf->position();
            }
            else if (*buf->position() == '\\')
            {
                ++buf->position();
                if (!buf->eof())
                {
                    ++buf->position();
                }
            }
        }
        else
        {
            pos = find_first_symbols<'"', '{', '}', '\\'>(buf->position(), buf->buffer().end());
            buf->position() = pos;
            if (buf->position() == buf->buffer().end())
                continue;
            if (*buf->position() == '{')
            {
                ++balance;
                ++buf->position();
            }
            else if (*buf->position() == '}')
            {
                --balance;
                ++buf->position();
            }
            else if (*buf->position() == '\\')
            {
                ++buf->position();
                if (!buf->eof())
                {
                    ++buf->position();
                }
            }
            else if (*buf->position() == '"')
            {
                quotes = true;
                ++buf->position();
            }
        }
    }
    buf->makeContinuousMemoryFromCheckpointToPos();
    char * end = buf->position();
    buf->rollbackToCheckpoint();
    column.insertData(buf->position(), end - buf->position());
    buf->position() = end;
}


JSONAsObjectRowInputFormat::JSONAsObjectRowInputFormat(
    SharedHeader header_, ReadBuffer & in_, Params params_, const FormatSettings & format_settings_)
    : JSONAsRowInputFormat(header_, in_, params_, format_settings_)
{
    const auto & type = header_->getByPosition(0).type;
    if (!isObject(type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Input format JSONAsObject is only suitable for tables with a single column of type JSON but the column type is {}",
            type->getName());
}

void JSONAsObjectRowInputFormat::readJSONObject(IColumn & column)
{
    serializations[0]->deserializeTextJSON(column, *in, format_settings);
}

Chunk JSONAsObjectRowInputFormat::getChunkForCount(size_t rows)
{
    auto object_type = getPort().getHeader().getDataTypes()[0];
    ColumnPtr column = object_type->createColumnConst(rows, Field(Object()));
    return Chunk({std::move(column)}, rows);
}

JSONAsObjectExternalSchemaReader::JSONAsObjectExternalSchemaReader(const FormatSettings & settings_) : settings(settings_)
{
}

void registerInputFormatJSONAsString(FormatFactory & factory);
void registerInputFormatJSONAsString(FormatFactory & factory)
{
    factory.registerInputFormat("JSONAsString", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings & format_settings)
    {
        return std::make_shared<JSONAsStringRowInputFormat>(std::make_unique<const Block>(sample), buf, params, format_settings);
    });

    factory.setDocumentation("JSONAsString", Documentation{
        .description = R"DOCS_MD(
| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

In this format, a single JSON object is interpreted as a single value. 
If the input has several JSON objects (which are comma separated), they are interpreted as separate rows. 
If the input data is enclosed in `[]`, it is interpreted as an array of JSON objects.

:::note
This format can only be parsed for a table with a single field of type [String](/sql-reference/data-types/string.md). 
The remaining columns must be set to either [`DEFAULT`](/sql-reference/statements/create/table.md/#default) or [`MATERIALIZED`](/sql-reference/statements/create/view#materialized-view), 
or be omitted. 
:::

Once you serialize the entire JSON object to a String you can use the [JSON functions](/sql-reference/functions/json-functions.md) to process it.

## Example usage {#example-usage}

### Basic example {#basic-example}

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

### An array of JSON objects {#an-array-of-json-objects}

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerFileSegmentationEngineJSONAsString(FormatFactory & factory);
void registerFileSegmentationEngineJSONAsString(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsString", &JSONUtils::fileSegmentationEngineJSONEachRow);
}

void registerNonTrivialPrefixAndSuffixCheckerJSONAsString(FormatFactory & factory);
void registerNonTrivialPrefixAndSuffixCheckerJSONAsString(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONAsString", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerJSONAsStringSchemaReader(FormatFactory & factory);
void registerJSONAsStringSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("JSONAsString", [](const FormatSettings &)
    {
        return std::make_shared<JSONAsStringExternalSchemaReader>();
    });
}

void registerInputFormatJSONAsObject(FormatFactory & factory);
void registerInputFormatJSONAsObject(FormatFactory & factory)
{
    factory.registerInputFormat("JSONAsObject", [](
        ReadBuffer & buf,
        const Block & sample,
        IRowInputFormat::Params params,
        const FormatSettings & settings)
    {
        return std::make_shared<JSONAsObjectRowInputFormat>(std::make_unique<const Block>(sample), buf, std::move(params), settings);
    });

    factory.setDocumentation("JSONAsObject", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

In this format, a single JSON object is interpreted as a single [JSON](/sql-reference/data-types/newjson.md) value. If the input has several JSON objects (comma separated), they are interpreted as separate rows. If the input data is enclosed in `[]`, it is interpreted as an array of JSONs.

This format can only be parsed for a table with a single field of type [JSON](/sql-reference/data-types/newjson.md). The remaining columns must be set to [`DEFAULT`](/sql-reference/statements/create/table.md/#default) or [`MATERIALIZED`](/sql-reference/statements/create/view#materialized-view).

## Example usage {#example-usage}

### Basic example {#basic-example}

```sql title="Query"
CREATE TABLE json_as_object (json JSON) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_object FORMAT JSONEachRow;
```

```response title="Response"
{"json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
{"json":{}}
{"json":{"any json stucture":"1"}}
```

### An array of JSON objects {#an-array-of-json-objects}

```sql title="Query"
CREATE TABLE json_square_brackets (field JSON) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsObject [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];
SELECT * FROM json_square_brackets FORMAT JSONEachRow;
```

```response title="Response"
{"field":{"id":"1","name":"name1"}}
{"field":{"id":"2","name":"name2"}}
```

### Columns with default values {#columns-with-default-values}

```sql title="Query"
CREATE TABLE json_as_object (json JSON, time DateTime MATERIALIZED now()) ENGINE = Memory;
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"foo":{"bar":{"x":"y"},"baz":1}};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {};
INSERT INTO json_as_object (json) FORMAT JSONAsObject {"any json stucture":1}
SELECT time, json FROM json_as_object FORMAT JSONEachRow
```

```response title="Response"
{"time":"2024-09-16 12:18:10","json":{}}
{"time":"2024-09-16 12:18:13","json":{"any json stucture":"1"}}
{"time":"2024-09-16 12:18:08","json":{"foo":{"bar":{"x":"y"},"baz":"1"}}}
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerNonTrivialPrefixAndSuffixCheckerJSONAsObject(FormatFactory & factory);
void registerNonTrivialPrefixAndSuffixCheckerJSONAsObject(FormatFactory & factory)
{
    factory.registerNonTrivialPrefixAndSuffixChecker("JSONAsObject", JSONUtils::nonTrivialPrefixAndSuffixCheckerJSONEachRowImpl);
}

void registerFileSegmentationEngineJSONAsObject(FormatFactory & factory);
void registerFileSegmentationEngineJSONAsObject(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("JSONAsObject", &JSONUtils::fileSegmentationEngineJSONEachRow);
}

void registerJSONAsObjectSchemaReader(FormatFactory & factory);
void registerJSONAsObjectSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("JSONAsObject", [](const FormatSettings & settings)
    {
        return std::make_shared<JSONAsObjectExternalSchemaReader>(settings);
    });
}

}
