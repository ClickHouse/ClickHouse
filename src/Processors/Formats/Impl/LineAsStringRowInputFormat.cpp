#include <Processors/Formats/Impl/LineAsStringRowInputFormat.h>
#include <base/find_symbols.h>
#include <IO/ReadHelpers.h>
#include <Columns/ColumnString.h>
#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

LineAsStringRowInputFormat::LineAsStringRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_) :
    IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_->columns() != 1
        || !typeid_cast<const ColumnString *>(header_->getByPosition(0).column.get()))
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "This input format is only suitable for tables with a single column of type String.");
    }
}

void LineAsStringRowInputFormat::resetParser()
{
    IRowInputFormat::resetParser();
}

void LineAsStringRowInputFormat::readLineObject(IColumn & column)
{
    ColumnString & column_string = assert_cast<ColumnString &>(column);
    auto & chars = column_string.getChars();
    auto & offsets = column_string.getOffsets();

    readStringUntilNewlineInto(chars, *in);
    offsets.push_back(chars.size());

    if (!in->eof())
        in->ignore(); /// Skip '\n'
}

bool LineAsStringRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    readLineObject(*columns[0]);
    return true;
}

size_t LineAsStringRowInputFormat::countRows(size_t max_block_size)
{
    size_t num_rows = 0;
    while (!in->eof() && num_rows < max_block_size)
    {
        skipToNextLineOrEOF(*in);
        ++num_rows;
    }

    return num_rows;
}

void registerInputFormatLineAsString(FormatFactory & factory);
void registerInputFormatLineAsString(FormatFactory & factory)
{
    factory.registerInputFormat("LineAsString", [](
        ReadBuffer & buf,
        const Block & sample,
        const RowInputFormatParams & params,
        const FormatSettings &)
    {
        return std::make_shared<LineAsStringRowInputFormat>(std::make_shared<const Block>(sample), buf, params);
    });

    factory.setDocumentation("LineAsString", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `LineAsString` format interprets every line of input data as a single string value. 
This format can only be parsed for a table with a single field of type [String](/sql-reference/data-types/string.md). 
The remaining columns must be set to [`DEFAULT`](/sql-reference/statements/create/table.md/#default), [`MATERIALIZED`](/sql-reference/statements/create/view#materialized-view), or omitted.

## Example usage {#example-usage}

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("LineAsStringWithNames", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format but prints the header row with column names.

## Example usage {#example-usage}

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNames;
```

```response title="Response"
name    value
John    30
Jane    25
Peter    35
```

## Format settings {#format-settings}
)DOCS_MD"});

    factory.setDocumentation("LineAsStringWithNamesAndTypes", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `LineAsStringWithNames` format is similar to the [`LineAsString`](./LineAsString.md) format 
but prints two header rows: one with column names, the other with types.

## Example usage {#example-usage}

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

## Format settings {#format-settings}
)DOCS_MD"});
}


static std::pair<bool, size_t> segmentationEngine(ReadBuffer & in, DB::Memory<> & memory, size_t min_bytes, size_t max_rows)
{
    char * pos = in.position();
    bool need_more_data = true;
    size_t number_of_rows = 0;

    while (loadAtPosition(in, memory, pos) && need_more_data)
    {
        pos = find_first_symbols<'\n'>(pos, in.buffer().end());
        if (pos > in.buffer().end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Position in buffer is out of bounds. There must be a bug.");
        if (pos == in.buffer().end())
            continue;

        ++number_of_rows;
        if ((memory.size() + static_cast<size_t>(pos - in.position()) >= min_bytes) || (number_of_rows == max_rows))
            need_more_data = false;

        if (*pos == '\n')
            ++pos;
    }

    saveUpToPosition(in, memory, pos);

    return {loadAtPosition(in, memory, pos), number_of_rows};
}

void registerFileSegmentationEngineLineAsString(FormatFactory & factory);
void registerFileSegmentationEngineLineAsString(FormatFactory & factory)
{
    factory.registerFileSegmentationEngine("LineAsString", &segmentationEngine);
}


void registerLineAsStringSchemaReader(FormatFactory & factory);
void registerLineAsStringSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("LineAsString", [](
        const FormatSettings &)
    {
        return std::make_shared<LinaAsStringSchemaReader>();
    });
}

}
