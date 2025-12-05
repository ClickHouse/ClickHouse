#include <Common/Exception.h>
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <Formats/BuffersReader.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int TOO_LARGE_ARRAY_SIZE;
}

BuffersReader::BuffersReader(ReadBuffer & istr_, const Block & header_, std::optional<FormatSettings> format_settings_)
    : istr(istr_)
    , header(header_)
    , format_settings(std::move(format_settings_))
{
    chassert(header.columns() > 0);
}

Block BuffersReader::getHeader() const
{
    return header;
}

void readData(
    const ISerialization & serialization,
    ColumnPtr & column,
    ReadBuffer & istr,
    UInt64 num_rows,
    const std::optional<FormatSettings> & format_settings)
{
    ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](ISerialization::SubstreamPath) -> ReadBuffer * { return &istr; };
    settings.position_independent_encoding = false;
    settings.native_format = false;
    settings.format_settings = format_settings.has_value() ? &format_settings.value() : nullptr;

    ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    serialization.deserializeBinaryBulkWithMultipleStreams(column, 0, num_rows, settings, state, nullptr);
}

Block BuffersReader::read()
{
    if (istr.eof())
        return Block{};

    /// Number of buffers (UInt64)
    UInt64 num_buffers = 0;
    readBinary(num_buffers, istr);

    if (num_buffers == 0)
        return Block{};

    if (num_buffers != header.columns())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Buffers block has {} buffers, but header has {} columns", num_buffers, header.columns());

    if (num_buffers > 100'000'000uz)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many buffers in Buffers format: {}", num_buffers);

    /// Number of rows (UInt64)
    UInt64 num_rows = 0;
    readBinary(num_rows, istr);

    if (num_rows > 1'000'000'000'000uz)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many rows in Buffers format: {}", num_rows);

    /// Size of each buffer (UInt64 * number of buffers)
    std::vector<UInt64> buffer_sizes(num_buffers);
    for (size_t i = 0; i < num_buffers; ++i)
        readBinary(buffer_sizes[i], istr);

    Block res;
    for (size_t i = 0; i < num_buffers; ++i)
    {
        auto column = header.getByPosition(i).cloneEmpty();

        ColumnPtr & read_column = column.column;

        auto serialization = column.type->getDefaultSerialization();

        const size_t before = istr.count();
        readData(*serialization, read_column, istr, num_rows, format_settings);
        const size_t after = istr.count();

        const size_t consumed = after - before;

        if (consumed != buffer_sizes[i])
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot read all data for column {} of type {} from Buffer at position {}, expected to read {} bytes, but read {} bytes",
                column.name,
                column.type->getName(),
                i,
                buffer_sizes[i],
                consumed);

        res.insert(std::move(column));
    }

    return res;
}

}
