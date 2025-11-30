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

void writeData(
    const ISerialization & serialization,
    const ColumnPtr & column,
    WriteBuffer & ostr,
    const std::optional<FormatSettings> & format_settings)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      * The same for compressed columns in-memory.
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst()->decompress();

    if (const auto * column_lazy = checkAndGetColumn<ColumnLazy>(full_column.get()))
    {
        const auto & columns = column_lazy->getColumns();
        full_column = ColumnTuple::create(columns);
    }

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0;
    settings.native_format = false;
    settings.format_settings = format_settings ? &*format_settings : nullptr;
    settings.dynamic_serialization_version = MergeTreeDynamicSerializationVersion::V2;
    settings.object_serialization_version = MergeTreeObjectSerializationVersion::V2;

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization.serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization.serializeBinaryBulkWithMultipleStreams(*full_column, 0, 0, settings, state);
    serialization.serializeBinaryBulkStateSuffix(settings, state);
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

        auto serialization = column.type->getDefaultSerialization();
        chassert(serialization != nullptr);

        WriteBufferFromOwnString buffer;

        writeData(*serialization, column.column, buffer, format_settings);

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
