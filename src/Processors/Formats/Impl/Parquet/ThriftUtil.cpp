#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

#include <Common/Exception.h>
#include <IO/WriteBuffer.h>

#include <fmt/format.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TProtocolException.h>
#include <thrift/transport/TBufferTransports.h>

#include <array>
#include <sstream>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
}

namespace DB::Parquet
{

class WriteBufferTransport : public apache::thrift::transport::TTransport
{
public:
    WriteBuffer & out;
    size_t bytes = 0;

    explicit WriteBufferTransport(WriteBuffer & out_) : out(out_) {}

    void write(const uint8_t* buf, uint32_t len)
    {
        out.write(reinterpret_cast<const char *>(buf), len);
        bytes += len;
    }
};

template <typename T>
size_t serializeThriftStruct(const T & obj, WriteBuffer & out)
{
    auto trans = std::make_shared<WriteBufferTransport>(out);
    apache::thrift::protocol::TCompactProtocolT<WriteBufferTransport> proto(trans);
    uint32_t bytes_written = obj.write(&proto);
    chassert(size_t(bytes_written) == trans->bytes);
    return size_t(bytes_written);
}

namespace
{

using MemoryTransport = apache::thrift::transport::TMemoryBuffer;
using CompactProtocol = apache::thrift::protocol::TCompactProtocolT<MemoryTransport>;

/// A container of N elements or an N-byte string occupies at least N bytes on the wire, and
/// only `limit` bytes exist, so no real length can exceed this while forged ones do.
int32_t sizeLimit(size_t limit)
{
    return int32_t(std::min(limit, size_t(std::numeric_limits<int32_t>::max())));
}

std::shared_ptr<MemoryTransport> makeTransport(const char * buf, size_t limit)
{
    /// TMemoryBuffer promises to not write to the buffer (in OBSERVE mode),
    /// so it should be ok to const_cast.
    uint8_t * cast_buf = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(buf));
    auto configuration = std::make_shared<apache::thrift::TConfiguration>(/*maxMessageSize=*/ sizeLimit(limit));
    return std::make_shared<MemoryTransport>(cast_buf, uint32_t(limit), MemoryTransport::OBSERVE, configuration);
}

template <typename F>
auto parseThrift(size_t limit, F && parse)
{
    try
    {
        return parse();
    }
    catch (const apache::thrift::TException & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "Failed to parse Parquet Thrift metadata from {} bytes: {}", limit, e.what());
    }
    catch (const std::exception & e)
    {
        /// Convert to DB::Exception for convenience, so that callers can catch+addMessage+rethrow.
        throw Exception(Exception::CreateFromSTDTag(), e);
    }
}

size_t countColumnChunks(const std::vector<UInt32> & offsets)
{
    return offsets.empty() ? 0 : offsets.size() - 1;
}

/// What a column chunk allocates outside of `sizeof(parq::ColumnChunk)`, approximately.
size_t columnChunkNestedMemory(const parq::ColumnChunk & column_chunk)
{
    size_t total_size = column_chunk.file_path.capacity();
    total_size += column_chunk.encrypted_column_metadata.capacity();

    const parq::ColumnMetaData & meta = column_chunk.meta_data;
    total_size += meta.path_in_schema.capacity() * sizeof(std::string);
    for (const auto & path : meta.path_in_schema)
        total_size += path.capacity();

    total_size += meta.key_value_metadata.capacity() * sizeof(parq::KeyValue);
    for (const auto & kv : meta.key_value_metadata)
        total_size += kv.key.capacity() + kv.value.capacity();

    total_size += meta.encodings.capacity() * sizeof(parq::Encoding::type);
    total_size += meta.encoding_stats.capacity() * sizeof(parq::PageEncodingStats);
    total_size += meta.geospatial_statistics.geospatial_types.capacity() * sizeof(int32_t);

    total_size += meta.statistics.min_value.capacity();
    total_size += meta.statistics.max_value.capacity();
    total_size += meta.statistics.min.capacity();
    total_size += meta.statistics.max.capacity();

    total_size += meta.size_statistics.repetition_level_histogram.capacity() * sizeof(int64_t);
    total_size += meta.size_statistics.definition_level_histogram.capacity() * sizeof(int64_t);

    const auto & column_key = column_chunk.crypto_metadata.ENCRYPTION_WITH_COLUMN_KEY;
    total_size += column_key.path_in_schema.capacity() * sizeof(std::string);
    for (const auto & path : column_key.path_in_schema)
        total_size += path.capacity();
    total_size += column_key.key_metadata.capacity();
    return total_size;
}

/// Hides the methods that the generated `FileMetaData::read` calls, to skip the elements of
/// `RowGroup::columns` instead of reading them. `skip` is `TVirtualProtocol<CompactProtocol>::skip`,
/// so skipped values never come back here.
class ColumnChunkSkippingProtocol : public CompactProtocol
{
public:
    ColumnChunkSkippingProtocol(std::shared_ptr<MemoryTransport> trans, size_t limit_, std::vector<std::vector<UInt32>> & offsets_)
        : CompactProtocol(trans, /*string_limit=*/ sizeLimit(limit_), /*container_limit=*/ sizeLimit(limit_))
        , limit(limit_)
        , offsets(offsets_)
    {
    }

    uint32_t readStructBegin(std::string & name)
    {
        ++depth;
        if (depth == 2 && field_ids[1] == FILE_META_DATA_ROW_GROUPS)
            offsets.emplace_back();
        return CompactProtocol::readStructBegin(name);
    }

    uint32_t readStructEnd()
    {
        --depth;
        return CompactProtocol::readStructEnd();
    }

    uint32_t readFieldBegin(std::string & name, apache::thrift::protocol::TType & type, int16_t & id)
    {
        uint32_t bytes = CompactProtocol::readFieldBegin(name, type, id);
        if (depth < field_ids.size())
            field_ids[depth] = id;
        return bytes;
    }

    uint32_t readListBegin(apache::thrift::protocol::TType & elem_type, uint32_t & size)
    {
        uint32_t bytes = CompactProtocol::readListBegin(elem_type, size);
        if (depth == 1 && field_ids[1] == FILE_META_DATA_ROW_GROUPS)
        {
            offsets.clear();
        }
        else if (depth == 2 && field_ids[1] == FILE_META_DATA_ROW_GROUPS && field_ids[2] == ROW_GROUP_COLUMNS)
        {
            chassert(!offsets.empty());
            auto & row_group_offsets = offsets.back();
            row_group_offsets.clear();
            row_group_offsets.reserve(size + 1);
            /// `skip` counts every value against the recursion limit, the generated reader only structs.
            /// One level less accepts every column chunk that the generated reader accepts.
            decrementInputRecursionDepth();
            for (uint32_t i = 0; i < size; ++i)
            {
                row_group_offsets.push_back(position());
                /// Read as a struct whatever the declared element type, like the generated reader.
                bytes += skip(apache::thrift::protocol::T_STRUCT);
            }
            row_group_offsets.push_back(position());
            incrementInputRecursionDepth();
            size = 0;
        }
        return bytes;
    }

private:
    static constexpr int16_t FILE_META_DATA_ROW_GROUPS = 4;
    static constexpr int16_t ROW_GROUP_COLUMNS = 1;

    UInt32 position() const { return UInt32(limit - trans_->available_read()); }

    const size_t limit;
    std::vector<std::vector<UInt32>> & offsets;
    /// Depth of the struct that the generated reader is reading, 1 for FileMetaData.
    size_t depth = 0;
    /// field_ids[d] is the id of the current field of the struct at depth d.
    std::array<int16_t, 3> field_ids {};
};

}

template <typename T>
size_t deserializeThriftStruct(T & out, const char * buf, size_t limit)
{
    limit = std::min(limit, size_t(UINT32_MAX));
    return parseThrift(limit, [&]
    {
        auto trans = makeTransport(buf, limit);
        CompactProtocol proto(trans, /*string_limit=*/ sizeLimit(limit), /*container_limit=*/ sizeLimit(limit));
        uint32_t bytes_read = out.read(&proto);
        chassert(size_t(bytes_read + trans->available_read()) == limit);
        return size_t(bytes_read);
    });
}

size_t FileFooter::numColumnChunks(size_t row_group_idx) const
{
    return countColumnChunks(column_chunk_offsets.at(row_group_idx));
}

void deserializeFileMetaDataWithoutColumnChunks(
    parq::FileMetaData & out, const char * buf, size_t limit, std::vector<std::vector<UInt32>> & column_chunk_offsets)
{
    limit = std::min(limit, size_t(UINT32_MAX));
    column_chunk_offsets.clear();
    parseThrift(limit, [&]
    {
        auto trans = makeTransport(buf, limit);
        ColumnChunkSkippingProtocol proto(trans, limit, column_chunk_offsets);
        uint32_t bytes_read = out.read(&proto);
        chassert(size_t(bytes_read + trans->available_read()) == limit);
    });
    if (column_chunk_offsets.size() != out.row_groups.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Found column chunks of {} Parquet row groups, expected {}",
            column_chunk_offsets.size(), out.row_groups.size());
}

void deserializeColumnChunks(
    const char * buf, size_t limit, const std::vector<std::vector<UInt32>> & column_chunk_offsets,
    const std::vector<size_t> & column_indices, std::vector<FileColumnChunks> & out)
{
    limit = std::min(limit, size_t(UINT32_MAX));
    out.clear();
    out.resize(column_indices.size());
    for (auto & column : out)
        column.chunks.reserve(column_chunk_offsets.size());
    parseThrift(limit, [&]
    {
        auto trans = makeTransport(buf, limit);
        CompactProtocol proto(trans, /*string_limit=*/ sizeLimit(limit), /*container_limit=*/ sizeLimit(limit));
        /// Read column chunks at the depth they have in FileMetaData: FileMetaData, RowGroup, ColumnChunk.
        apache::thrift::protocol::TInputRecursionTracker file_metadata_level(proto);
        apache::thrift::protocol::TInputRecursionTracker row_group_level(proto);
        size_t pos = 0;
        for (size_t row_group_idx = 0; row_group_idx < column_chunk_offsets.size(); ++row_group_idx)
        {
            const auto & offsets = column_chunk_offsets[row_group_idx];
            const size_t num_chunks = countColumnChunks(offsets);
            for (size_t k = 0; k < column_indices.size(); ++k)
            {
                const size_t column_idx = column_indices[k];
                auto & column_chunk = out[k].chunks.emplace_back();
                if (column_idx < num_chunks)
                {
                    chassert(offsets[column_idx] >= pos);
                    trans->consume(uint32_t(offsets[column_idx] - pos));
                    column_chunk.read(&proto);
                    pos = limit - trans->available_read();
                    if (pos != offsets[column_idx + 1])
                        throw apache::thrift::protocol::TProtocolException(
                            apache::thrift::protocol::TProtocolException::INVALID_DATA,
                            fmt::format("column chunk {} of row group {} ends at byte {} instead of {}",
                                column_idx, row_group_idx, pos, offsets[column_idx + 1]));
                }
                out[k].memory_bytes += columnChunkNestedMemory(column_chunk);
            }
        }
    });

    for (auto & column : out)
        column.memory_bytes += column.chunks.capacity() * sizeof(parq::ColumnChunk);
}

template <typename T>
std::string thriftToString(const T & obj)
{
    std::stringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss << obj;
    return ss.str();
}

template size_t serializeThriftStruct<parq::PageHeader>(const parq::PageHeader &, WriteBuffer & out);
template size_t serializeThriftStruct<parq::ColumnChunk>(const parq::ColumnChunk &, WriteBuffer & out);
template size_t serializeThriftStruct<parq::FileMetaData>(const parq::FileMetaData &, WriteBuffer & out);
template size_t serializeThriftStruct<parq::ColumnIndex>(const parq::ColumnIndex &, WriteBuffer & out);
template size_t serializeThriftStruct<parq::OffsetIndex>(const parq::OffsetIndex &, WriteBuffer & out);
template size_t serializeThriftStruct<parq::BloomFilterHeader>(const parq::BloomFilterHeader &, WriteBuffer & out);

template size_t deserializeThriftStruct<parq::FileMetaData>(parq::FileMetaData &, const char *, size_t);
template size_t deserializeThriftStruct<parq::PageHeader>(parq::PageHeader &, const char *, size_t);
template size_t deserializeThriftStruct<parq::BloomFilterHeader>(parq::BloomFilterHeader &, const char *, size_t);
template size_t deserializeThriftStruct<parq::ColumnIndex>(parq::ColumnIndex &, const char *, size_t);
template size_t deserializeThriftStruct<parq::OffsetIndex>(parq::OffsetIndex &, const char *, size_t);

template std::string thriftToString<parq::Encoding::type>(const parq::Encoding::type &);
template std::string thriftToString<parq::PageHeader>(const parq::PageHeader &);
template std::string thriftToString<parq::CompressionCodec::type>(const parq::CompressionCodec::type &);
template std::string thriftToString<parq::SchemaElement>(const parq::SchemaElement &);
template std::string thriftToString<parq::PageType::type>(const parq::PageType::type &);
template std::string thriftToString<parq::PageLocation>(const parq::PageLocation &);
template std::string thriftToString<parq::Type::type>(const parq::Type::type &);

}
