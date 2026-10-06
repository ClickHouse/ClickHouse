
#include <Storages/System/StorageSystemCodecs.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Compression/CompressionFactory.h>


namespace DB
{

ColumnsDescription StorageSystemCodecs::getColumnsDescription()
{
    return ColumnsDescription
    {
        { "name",                   std::make_shared<DataTypeString>(), "Codec name."},
        { "method_byte",            std::make_shared<DataTypeUInt8>(), "Byte which indicates codec in compressed file."},
        { "is_compression",         std::make_shared<DataTypeUInt8>(), "True if this codec compresses something. Otherwise it can be just a transformation that helps compression."},
        { "is_generic_compression", std::make_shared<DataTypeUInt8>(), "The codec is a generic compression algorithm like lz4, zstd."},
        { "is_encryption",          std::make_shared<DataTypeUInt8>(), "The codec encrypts."},
        { "is_timeseries_codec",    std::make_shared<DataTypeUInt8>(), "The codec is for floating point timeseries codec."},
        { "is_experimental",        std::make_shared<DataTypeUInt8>(), "The codec is experimental."},
        { "description",            std::make_shared<DataTypeString>(), "A high-level description of the codec."},
    };
}

void StorageSystemCodecs::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    CompressionCodecFactory::instance().fillCodecDescriptions(res_columns);
}

}
