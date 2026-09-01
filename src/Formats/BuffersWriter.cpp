#include <Formats/BuffersWriter.h>
#include <Formats/NativeWriter.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Core/Block.h>

namespace DB
{

BuffersWriter::BuffersWriter(WriteBuffer & ostr_, SharedHeader header_, const FormatSettings & format_settings_)
    : ostr(ostr_)
    , header(header_)
    , format_settings(format_settings_)
{
}

void BuffersWriter::write(const Block & block)
{
    block.checkNumberOfRows();

    const UInt64 num_columns = static_cast<UInt64>(block.columns());

    /// Number of buffers (UInt64)
    writeBinaryLittleEndian(num_columns, ostr);

    const UInt64 num_rows = static_cast<UInt64>(block.rows());

    /// Number of rows (UInt64)
    /// We encode number of rows because we need it to deserialize each column
    writeBinaryLittleEndian(num_rows, ostr);

    for (size_t i = 0; i < num_columns; ++i)
    {
        const auto & column = block.safeGetByPosition(i);

        SerializationPtr serialization = column.type->getDefaultSerialization();

        WriteBufferFromOwnString buffer;

        NativeWriter::writeData(*serialization, column.column, buffer, format_settings, 0, 0, format_settings.client_protocol_version);

        /// Size of buffer in bytes (UInt64)
        UInt64 buffer_size = static_cast<UInt64>(buffer.count());
        writeBinaryLittleEndian(buffer_size, ostr);

        /// Contents of buffer (raw bytes)
        ostr.write(buffer.str().data(), buffer_size);
    }
}

}
