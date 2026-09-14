#include <Columns/IColumn.h>
#include <Core/Defines.h>
#include <DataTypes/IDataType.h>
#include <Formats/BuffersReader.h>
#include <Formats/NativeReader.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int TOO_LARGE_ARRAY_SIZE;
}

BuffersReader::BuffersReader(ReadBuffer & istr_, const Block & header_, const FormatSettings & format_settings_)
    : istr(istr_)
    , header(header_)
    , format_settings(format_settings_)
{
    chassert(header.columns() > 0);
}

Block BuffersReader::getHeader() const
{
    return header;
}

Block BuffersReader::read()
{
    if (istr.eof())
        return Block{};

    /// Number of buffers (UInt64)
    UInt64 num_columns = 0;
    readBinaryLittleEndian(num_columns, istr);

    if (num_columns != header.columns())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Buffers block has {} buffers, but header has {} columns", num_columns, header.columns());

    if (num_columns > DEFAULT_NATIVE_BINARY_MAX_NUM_COLUMNS)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many buffers in Buffers format: {}", num_columns);

    /// Number of rows (UInt64)
    UInt64 num_rows = 0;
    readBinaryLittleEndian(num_rows, istr);

    if (num_rows > DEFAULT_NATIVE_BINARY_MAX_NUM_ROWS)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Suspiciously many rows in Buffers format: {}", num_rows);

    Block res;
    for (size_t i = 0; i < num_columns; ++i)
    {
        UInt64 buffer_size = 0;
        readBinaryLittleEndian(buffer_size, istr);

        auto column = header.getByPosition(i).cloneEmpty();

        ColumnPtr & read_column = column.column;

        auto serialization = column.type->getDefaultSerialization();

        const size_t before = istr.count();

        NameAndTypePair name_and_type = {column.name, column.type};
        NativeReader::readData(*serialization, read_column, istr, &format_settings, num_rows, &name_and_type, nullptr);

        const size_t after = istr.count();

        const size_t consumed = after - before;

        if (consumed != buffer_size)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Cannot read all data for column {} of type {} from Buffer at position {}, expected to read {} bytes, but read {} bytes",
                column.name,
                column.type->getName(),
                i,
                buffer_size,
                consumed);

        res.insert(std::move(column));
    }

    return res;
}

}
