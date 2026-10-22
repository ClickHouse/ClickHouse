#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>

#include <Common/Exception.h>
#include <base/defines.h>
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

/// Skips compact protocol values on the raw bytes. Accepts, rejects and consumes the same input as
/// `apache::thrift::protocol::skip` on `CompactProtocol` with the same size and recursion limits.
class CompactValueSkipper
{
public:
    CompactValueSkipper(const uint8_t * end_, int32_t size_limit_, uint32_t recursion_limit_)
        : end(end_), size_limit(size_limit_), recursion_limit(recursion_limit_)
    {
    }

    /// `depth` is the recursion depth of the struct itself, as counted by TInputRecursionTracker,
    /// which also counts every field value one level deeper.
    const uint8_t * skipStruct(const uint8_t * pos, uint32_t depth) const
    {
        if (depth > recursion_limit) [[unlikely]]
            throwDepthLimit();
        while (true)
        {
            need(pos, 1);
            const uint8_t header = *pos++;
            const uint8_t type = header & 0x0f;
            if (type == STOP)
                return pos;
            if (depth >= recursion_limit) [[unlikely]]
                throwDepthLimit();
            /// A zero delta means that the field id follows as a varint.
            if ((header >> 4) == 0)
                pos = skipVarint(pos);
            switch (type)
            {
                case BOOLEAN_TRUE:
                case BOOLEAN_FALSE:
                    /// The value of a bool field is in the field header.
                    break;
                case I16:
                case I32:
                case I64:
                    pos = skipVarint(pos);
                    break;
                case BINARY:
                    pos = skipBinary(pos);
                    break;
                default:
                    pos = skipValue(pos, type, depth + 1);
                    break;
            }
        }
    }

private:
    enum : uint8_t
    {
        STOP = 0x00, BOOLEAN_TRUE = 0x01, BOOLEAN_FALSE = 0x02, BYTE = 0x03, I16 = 0x04, I32 = 0x05, I64 = 0x06,
        DOUBLE = 0x07, BINARY = 0x08, LIST = 0x09, SET = 0x0A, MAP = 0x0B, STRUCT = 0x0C, UUID = 0x0D,
    };

    const uint8_t * const end;
    const int32_t size_limit;
    const uint32_t recursion_limit;

    [[noreturn]] static NO_INLINE void throwInvalidData(const char * message)
    {
        throw apache::thrift::protocol::TProtocolException(apache::thrift::protocol::TProtocolException::INVALID_DATA, message);
    }

    [[noreturn]] static NO_INLINE void throwDepthLimit()
    {
        throw apache::thrift::protocol::TProtocolException(apache::thrift::protocol::TProtocolException::DEPTH_LIMIT);
    }

    [[noreturn]] static NO_INLINE void throwBadSize(bool negative)
    {
        throw apache::thrift::protocol::TProtocolException(
            negative ? apache::thrift::protocol::TProtocolException::NEGATIVE_SIZE : apache::thrift::protocol::TProtocolException::SIZE_LIMIT);
    }

    [[noreturn]] static NO_INLINE void throwUnknownType(uint8_t type)
    {
        throw apache::thrift::TException(std::string("don't know what type: ") + static_cast<char>(type));
    }

    ALWAYS_INLINE void need(const uint8_t * pos, size_t bytes) const
    {
        if (static_cast<size_t>(end - pos) < bytes) [[unlikely]]
            throwInvalidData("Unexpected end of Thrift data");
    }

    static ALWAYS_INLINE void checkType(uint8_t type)
    {
        if (type > UUID) [[unlikely]]
            throwUnknownType(type);
    }

    ALWAYS_INLINE const uint8_t * skipVarint(const uint8_t * pos) const
    {
        /// Like TCompactProtocol::readVarint64: at most 10 bytes.
        for (size_t i = 0; i < 10; ++i)
        {
            need(pos, 1);
            if (!(*pos++ & 0x80))
                return pos;
        }
        throwInvalidData("Variable-length int over 10 bytes.");
    }

    /// Like TCompactProtocol::readVarint32 and its callers: a varint of up to 10 bytes truncated to 32 bits.
    ALWAYS_INLINE const uint8_t * readSize(const uint8_t * pos, int32_t & size) const
    {
        uint64_t value = 0;
        for (size_t i = 0; i < 10; ++i)
        {
            need(pos, 1);
            const uint8_t byte = *pos++;
            value |= static_cast<uint64_t>(byte & 0x7f) << (7 * i);
            if (!(byte & 0x80))
            {
                size = static_cast<int32_t>(static_cast<uint32_t>(value));
                if (size < 0 || (size_limit > 0 && size > size_limit)) [[unlikely]]
                    throwBadSize(size < 0);
                return pos;
            }
        }
        throwInvalidData("Variable-length int over 10 bytes.");
    }

    ALWAYS_INLINE const uint8_t * skipBinary(const uint8_t * pos) const
    {
        int32_t size = 0;
        pos = readSize(pos, size);
        need(pos, size_t(size));
        return pos + size;
    }

    /// Skips `count` elements of a list or set, at `depth`.
    const uint8_t * skipElements(const uint8_t * pos, uint8_t type, int32_t count, uint32_t depth) const
    {
        if (count == 0)
            return pos;
        if (type == STOP) [[unlikely]]
            throwInvalidData("invalid TType");
        if (depth > recursion_limit) [[unlikely]]
            throwDepthLimit();
        switch (type)
        {
            case BOOLEAN_TRUE:
            case BOOLEAN_FALSE:
            case BYTE:
                need(pos, size_t(count));
                return pos + count;
            case I16:
            case I32:
            case I64:
                for (int32_t i = 0; i < count; ++i)
                    pos = skipVarint(pos);
                return pos;
            case BINARY:
                for (int32_t i = 0; i < count; ++i)
                    pos = skipBinary(pos);
                return pos;
            default:
                for (int32_t i = 0; i < count; ++i)
                    pos = skipValue(pos, type, depth);
                return pos;
        }
    }

    /// Skips a value of any type except STOP; the value is at `depth`.
    const uint8_t * skipValue(const uint8_t * pos, uint8_t type, uint32_t depth) const
    {
        if (depth > recursion_limit) [[unlikely]]
            throwDepthLimit();
        switch (type)
        {
            case BOOLEAN_TRUE:
            case BOOLEAN_FALSE:
            case BYTE:
                /// Only reached for elements of containers: a bool element takes one byte.
                need(pos, 1);
                return pos + 1;
            case I16:
            case I32:
            case I64:
                return skipVarint(pos);
            case DOUBLE:
                need(pos, 8);
                return pos + 8;
            case UUID:
                need(pos, 16);
                return pos + 16;
            case BINARY:
                return skipBinary(pos);
            case STRUCT:
                return skipStruct(pos, depth);
            case LIST:
            case SET:
            {
                need(pos, 1);
                const uint8_t header = *pos++;
                int32_t count = (header >> 4) & 0x0f;
                if (count == 15)
                    pos = readSize(pos, count);
                else if (size_limit > 0 && count > size_limit) [[unlikely]]
                    throwBadSize(false);
                const uint8_t element_type = header & 0x0f;
                checkType(element_type);
                return skipElements(pos, element_type, count, depth + 1);
            }
            case MAP:
            {
                int32_t count = 0;
                pos = readSize(pos, count);
                if (count == 0)
                    return pos;
                need(pos, 1);
                const uint8_t types = *pos++;
                const uint8_t key_type = types >> 4;
                const uint8_t value_type = types & 0x0f;
                checkType(key_type);
                checkType(value_type);
                if (key_type == STOP || value_type == STOP) [[unlikely]]
                    throwInvalidData("invalid TType");
                if (depth >= recursion_limit) [[unlikely]]
                    throwDepthLimit();
                for (int32_t i = 0; i < count; ++i)
                {
                    pos = skipValue(pos, key_type, depth + 1);
                    pos = skipValue(pos, value_type, depth + 1);
                }
                return pos;
            }
            default:
                checkType(type);
                throwInvalidData("invalid TType");
        }
    }
};

/// Hides the methods that the generated `FileMetaData::read` calls, to skip the elements of
/// `RowGroup::columns` instead of reading them. The elements are skipped by `CompactValueSkipper`,
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
            uint32_t available = 0;
            const uint8_t * begin = trans_->borrow(nullptr, &available);
            const UInt32 begin_position = position();
            const CompactValueSkipper skipper(begin + available, sizeLimit(limit), getRecursionLimit());
            const uint8_t * pos = begin;
            for (uint32_t i = 0; i < size; ++i)
            {
                row_group_offsets.push_back(begin_position + UInt32(pos - begin));
                /// Read as a struct whatever the declared element type, like the generated reader.
                /// Depth 2, one less than the generated reader's: `skip` counts every value against the
                /// recursion limit, the generated reader only structs.
                pos = skipper.skipStruct(pos, /*depth=*/ 2);
            }
            const auto skipped = UInt32(pos - begin);
            trans_->consume(skipped);
            bytes += skipped;
            row_group_offsets.push_back(position());
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
