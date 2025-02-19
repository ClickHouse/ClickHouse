#include <Processors/Formats/Impl/Parquet/ThriftUtil.h>
#include <thrift/protocol/TCompactProtocol.h>

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
    auto proto = apache::thrift::protocol::TCompactProtocolFactoryT<WriteBufferTransport>().getProtocol(trans);
    obj.write(proto.get());
    return trans->bytes;
}

template size_t serializeThriftStruct<parquet::format::PageHeader>(const parquet::format::PageHeader &, WriteBuffer & out);
template size_t serializeThriftStruct<parquet::format::ColumnChunk>(const parquet::format::ColumnChunk &, WriteBuffer & out);
template size_t serializeThriftStruct<parquet::format::FileMetaData>(const parquet::format::FileMetaData &, WriteBuffer & out);
template size_t serializeThriftStruct<parquet::format::ColumnIndex>(const parquet::format::ColumnIndex &, WriteBuffer & out);
template size_t serializeThriftStruct<parquet::format::OffsetIndex>(const parquet::format::OffsetIndex &, WriteBuffer & out);

}
