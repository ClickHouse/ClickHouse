#include <Formats/BuffersReader.h>
#include <Formats/BuffersWriter.h>
#include <Formats/FormatFactory.h>
#include <IO/WithFileSize.h>

#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{


class BuffersInputFormat final : public IInputFormat
{
public:
    BuffersInputFormat(ReadBuffer & buf, SharedHeader header_, const FormatSettings & settings_)
        : IInputFormat(header_, &buf)
        , reader(std::make_unique<BuffersReader>(buf, *header_, settings_))
        , header(header_)
        , settings(settings_)
    {
    }

    String getName() const override { return "Buffers"; }

    void resetParser() override { IInputFormat::resetParser(); }

    Chunk read() override
    {
        size_t block_start = getDataOffsetMaybeCompressed(*in);
        auto block = reader->read();
        approx_bytes_read_for_chunk = getDataOffsetMaybeCompressed(*in) - block_start;

        if (block.empty())
            return {};

        assertBlocksHaveEqualStructure(getPort().getHeader(), block, getName());
        block.checkNumberOfRows();

        size_t num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    void setReadBuffer(ReadBuffer & in_) override
    {
        reader = std::make_unique<BuffersReader>(in_, *header, settings);
        IInputFormat::setReadBuffer(in_);
    }

    size_t getApproxBytesReadForChunk() const override { return approx_bytes_read_for_chunk; }

private:
    std::unique_ptr<BuffersReader> reader;
    SharedHeader header;
    const FormatSettings settings;
    size_t approx_bytes_read_for_chunk = 0;
};


class BuffersOutputFormat final : public IOutputFormat
{
public:
    BuffersOutputFormat(WriteBuffer & buf, SharedHeader header, const FormatSettings & settings)
        : IOutputFormat(header, buf)
        , writer(buf, header, settings)
    {
    }

    String getName() const override { return "Buffers"; }

protected:
    void consume(Chunk chunk) override
    {
        if (!chunk)
            return;

        auto block = getPort(PortKind::Main).getHeader();
        block.setColumns(chunk.detachColumns());
        writer.write(block);
    }

private:
    BuffersWriter writer;
};

void registerInputFormatBuffers(FormatFactory & factory);
void registerInputFormatBuffers(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Buffers",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<BuffersInputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.setDocumentation("Buffers", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`Buffers` is a very simple binary format for **ephemeral** data exchange, where both the consumer and producer already know the schema and column order.

Unlike [Native](./Native.md), it does **not** store column names, column types, or any extra metadata.

In this format, data is written and read by [blocks](/development/architecture#block) in a binary format. Buffers uses the same per-column binary representation as the [Native](./Native.md) format and respects the same Native format settings.

For each block, the following sequence is written:
1. Number of columns (UInt64, little-endian).
2. Number of rows (UInt64, little-endian).
3. For each column:
- Total byte size of the serialized column data (UInt64, little-endian).
- Serialized column data bytes, exactly as in the [Native](./Native.md) format.

## Example usage {#example-usage}

Write to a file:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

Read back with an explicit column types:

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

If you have a table with same column types, you can populate it directly:

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

Inspect the table:

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerOutputFormatBuffers(FormatFactory & factory);
void registerOutputFormatBuffers(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "Buffers",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<BuffersOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.markOutputFormatNotTTYFriendly("Buffers");
    factory.setContentType("Buffers", "application/octet-stream");
}

}
