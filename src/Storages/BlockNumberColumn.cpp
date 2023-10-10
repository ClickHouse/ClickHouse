#include <Storages/BlockNumberColumn.h>
#include <Compression/CompressionCodecMultiple.h>

namespace DB
{

CompressionCodecPtr getCompressionCodecDelta(UInt8 delta_bytes_size);

CompressionCodecPtr getCompressionCodecForBlockNumberColumn()
{
    std::vector <CompressionCodecPtr> codecs;
    codecs.reserve(2);
    auto data_bytes_size = BlockNumberColumn::type->getSizeOfValueInMemory();
    codecs.emplace_back(getCompressionCodecDelta(data_bytes_size));
    codecs.emplace_back(CompressionCodecFactory::instance().get("LZ4", {}));
    return std::make_shared<CompressionCodecMultiple>(codecs);
}

const String BlockNumberColumn::name = "_block_number";
const DataTypePtr BlockNumberColumn::type = std::make_shared<DataTypeUInt64>();
const CompressionCodecPtr BlockNumberColumn::compression_codec = getCompressionCodecForBlockNumberColumn();

}
