#pragma once

#include <Core/Block_fwd.h>
#include <DataTypes/IDataType.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeWriterTypeCache.h>

namespace DB
{

struct ColumnWithTypeAndName;
class WriteBuffer;
class CompressedWriteBuffer;
struct IndexForNativeFormat;

/** Serializes the stream of blocks in their native binary format (with names and column types).
  * Designed for communication between servers.
  *
  * A stream can be specified to write the index. The index contains offsets to each part of each column.
  * If an `append` is made to an existing file, and you need to write the index, then specify `initial_size_of_file`.
  */
class NativeWriter
{
public:
    /** If non-zero client_revision is specified, additional block information can be written.
      */
    NativeWriter(
        WriteBuffer & ostr_, UInt64 client_revision_, SharedHeader header_, std::optional<FormatSettings> format_settings_ = std::nullopt, bool remove_low_cardinality_ = false,
        IndexForNativeFormat * index_ = nullptr, size_t initial_size_of_file_ = 0, NativeWriterTypeCache * type_cache_ = nullptr);

    SharedHeader getHeader() const { return header; }

    /// Returns the number of bytes written.
    size_t write(const Block & block);
    void flush();

    static std::tuple<SerializationPtr, SerializationInfoPtr, ColumnPtr> getSerializationAndColumn(UInt64 client_revision, const ColumnWithTypeAndName & column);
    static SerializationInfoSettings getSerializationInfoSettings(UInt64 client_revision);

    /// Sets the state versions of the aggregate functions in `type` to the ones announced to a peer at `client_revision`.
    static void setAggregateFunctionStateVersions(DataTypePtr & type, UInt64 client_revision);

    static void writeData(const ISerialization & serialization, const ColumnPtr & column, WriteBuffer & ostr, const std::optional<FormatSettings> & format_settings, UInt64 offset, UInt64 limit, UInt64 client_revision);

private:
    /// The entry of `type_cache` for a column type of the block. It is made when the type is new.
    NativeWriterTypeCache::Entry & getCachedType(const DataTypePtr & type);
    /// The serialization for `info` of a column with the type of `entry`. It is made when the kinds are not in `entry`.
    static SerializationPtr getCachedSerialization(NativeWriterTypeCache::Entry & entry, const SerializationInfo & info);

    WriteBuffer & ostr;
    UInt64 client_revision;
    SharedHeader header;
    IndexForNativeFormat * index = nullptr;
    size_t initial_size_of_file;    /// The initial size of the data file, if `append` done. Used for the index.
    /// If you need to write index, then `ostr` must be a CompressedWriteBuffer.
    CompressedWriteBuffer * ostr_concrete = nullptr;

    bool remove_low_cardinality;
    std::optional<FormatSettings> format_settings;
    NativeWriterTypeCache * type_cache = nullptr;
};

}
