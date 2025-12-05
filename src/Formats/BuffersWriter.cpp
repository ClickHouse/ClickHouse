#include <Formats/BuffersWriter.h>
#include <Formats/NativeWriter.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnLazy.h>
#include <Columns/ColumnTuple.h>

#include <Core/Block.h>

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

    /// Serialize each column into its own in-memory buffer first so that
    /// we know sizes before writing num_buffers + sizes
    std::vector<std::string> column_buffers(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
    {
        auto column = block.safeGetByPosition(i);

        SerializationPtr serialization;

        std::tie(serialization, std::ignore, column.column)
            = NativeWriter::getSerializationAndColumn(format_settings->client_protocol_version, column);

        WriteBufferFromOwnString buffer;

        NativeWriter::writeData(*serialization, column.column, buffer, format_settings, 0, 0, format_settings->client_protocol_version);

        column_buffers[i] = std::move(buffer.str());
    }

    const UInt64 num_buffers = static_cast<UInt64>(column_buffers.size());

    /// Number of buffers (UInt64)
    writeBinary(num_buffers, ostr);

    const UInt64 num_rows = static_cast<UInt64>(block.rows());

    /// Number of rows (UInt64)
    /// We encode number of rows because we need it to deserialize each column
    writeBinary(num_rows, ostr);

    /// Size of each buffer in bytes (UInt64)
    for (const auto & data : column_buffers)
    {
        const UInt64 sz = static_cast<UInt64>(data.size());
        writeBinary(sz, ostr);
    }

    /// Contents of each buffer (raw bytes, concatenated)
    for (const auto & data : column_buffers)
    {
        ostr.write(data.data(), data.size());
    }

    const size_t written_after = ostr.count();
    return written_after - written_before;
}

}
