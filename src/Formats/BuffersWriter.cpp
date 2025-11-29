#include <Core/Block.h>
#include <Formats/BuffersWriter.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnLazy.h>
#include <Columns/ColumnTuple.h>

namespace DB
{

BuffersWriter::BuffersWriter(WriteBuffer & ostr_, SharedHeader header_, std::optional<FormatSettings> format_settings_)
    : ostr(ostr_)
    , header(header_)
    , format_settings(std::move(format_settings_))
{
}

size_t BuffersWriter::write(const Block & block)
{
    const size_t written_before = ostr.count();

    block.checkNumberOfRows();

    const size_t num_columns = block.columns();
    const size_t rows = block.rows();

    /// Serialize each column into its own in-memory buffer first so that
    /// we know sizes before writing num_buffers + sizes
    std::vector<std::string> column_buffers(num_columns);

    FormatSettings default_settings;
    const FormatSettings & used_settings = format_settings ? *format_settings : default_settings;

    for (size_t i = 0; i < num_columns; ++i)
    {
        auto column = block.safeGetByPosition(i);

        /// Materialize const and decompress in-memory compressed columns
        ColumnPtr full_column = column.column->convertToFullColumnIfConst()->decompress();

        if (const auto * column_lazy = checkAndGetColumn<ColumnLazy>(full_column.get()))
        {
            const auto & columns = column_lazy->getColumns();
            full_column = ColumnTuple::create(columns);
        }

        auto serialization = column.type->getDefaultSerialization();
        chassert(serialization != nullptr);

        WriteBufferFromOwnString buffer;

        /// Row-by-row serialization:
        /// buffer = serializeBinary(value_0), serializeBinary(value_1), ...
        for (size_t row = 0; row < rows; ++row)
            serialization->serializeBinary(*full_column, row, buffer, used_settings);

        column_buffers[i] = buffer.str();
    }

    const UInt64 num_buffers = static_cast<UInt64>(column_buffers.size());

    /// Number of buffers (UInt64)
    writeBinary(num_buffers, ostr);

    /// Size of each buffer (UInt64 * number of buffers)
    for (const auto & data : column_buffers)
    {
        const UInt64 sz = static_cast<UInt64>(data.size());
        writeBinary(sz, ostr);
    }

    /// Contents of each buffer (raw bytes, concatenated)
    for (const auto & data : column_buffers)
    {
        if (!data.empty())
            ostr.write(data.data(), data.size());
    }

    const size_t written_after = ostr.count();
    return written_after - written_before;
}

}
