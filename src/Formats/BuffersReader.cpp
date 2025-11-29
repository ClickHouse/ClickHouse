#include <Formats/BuffersReader.h>

#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <optional>
#include <string>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
extern const int CANNOT_READ_ALL_DATA;
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

Block BuffersReader::read()
{
    Block res;

    if (istr.eof())
        return res;

    /// Number of buffers (UInt64)
    UInt64 num_buffers = 0;
    readBinary(num_buffers, istr);

    if (num_buffers == 0)
        return Block{};

    if (num_buffers != header.columns())
        throw Exception(
            ErrorCodes::INCORRECT_DATA, "Buffers block has {} buffers, but header has {} columns", num_buffers, header.columns());

    /// Size of each buffer (UInt64 * number of buffers)
    std::vector<UInt64> buffer_sizes(num_buffers);
    for (size_t i = 0; i < num_buffers; ++i)
        readBinary(buffer_sizes[i], istr);

    /// Read raw bytes for each buffer
    std::vector<std::string> raw_buffers(num_buffers);
    for (size_t i = 0; i < num_buffers; ++i)
    {
        raw_buffers[i].resize(buffer_sizes[i]);
        if (buffer_sizes[i] != 0)
            istr.readStrict(raw_buffers[i].data(), buffer_sizes[i]);
    }

    FormatSettings default_settings;
    const FormatSettings & used_settings = format_settings ? *format_settings : default_settings;

    size_t rows = 0;
    bool first_column = true;

    for (size_t i = 0; i < num_buffers; ++i)
    {
        const auto & header_col = header.getByPosition(i);

        ColumnWithTypeAndName column;
        column.name = header_col.name;
        column.type = header_col.type;

        /// Create mutable column for deserialization
        auto mutable_column = column.type->createColumn();

        size_t column_rows = 0;

        if (!raw_buffers[i].empty())
        {
            chassert(buffer_sizes[i] == raw_buffers[i].size());

            ReadBufferFromString column_buf(raw_buffers[i]);

            auto serialization = column.type->getDefaultSerialization();

            /// Read row-by-row until the buffer is exhausted
            while (!column_buf.eof())
            {
                size_t old_size = mutable_column->size();
                serialization->deserializeBinary(*mutable_column, column_buf, used_settings);

                if (mutable_column->size() == old_size)
                    throw Exception(
                        ErrorCodes::CANNOT_READ_ALL_DATA, "Serialization for column {} in Buffers format did not advance", column.name);

                ++column_rows;
            }
        }

        if (first_column)
        {
            rows = column_rows;
            first_column = false;
        }
        else if (column_rows != rows)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Column {} in Buffers block has {} rows, but expected {}", column.name, column_rows, rows);
        }

        column.column = std::move(mutable_column);
        res.insert(std::move(column));
    }

    return res;
}

}
