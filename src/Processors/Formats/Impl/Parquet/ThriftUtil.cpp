#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

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

template <typename T>
size_t deserializeThriftStruct(T & out, const char * buf, size_t limit)
{
    limit = std::min(limit, size_t(UINT32_MAX));
    /// TMemoryBuffer promises to not write to the buffer (in OBSERVE mode),
    /// so it should be ok to const_cast.
    auto cast_buf = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(buf));
    auto trans = std::make_shared<apache::thrift::transport::TMemoryBuffer>(cast_buf, uint32_t(limit));
    apache::thrift::protocol::TCompactProtocolT<apache::thrift::transport::TMemoryBuffer> proto(trans);
    uint32_t bytes_read = out.read(&proto);
    chassert(size_t(bytes_read + trans->available_read()) == limit);
    return size_t(bytes_read);
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

}
