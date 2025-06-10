
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
        { "codec",                     std::make_shared<DataTypeString>(), "Codec name."},
        { "method_byte",               std::make_shared<DataTypeUInt8>(), "Byte which indicates codec in compressed file."},
        { "compression",               std::make_shared<DataTypeUInt8>(), "The codec is a compression."},
        { "generic_compression",       std::make_shared<DataTypeUInt8>(), "The codec is a generic compression."},
        { "encription",                std::make_shared<DataTypeUInt8>(), "The codec encrypts."},
        { "floating_point_timeseries", std::make_shared<DataTypeUInt8>(), "The codec is for floating point timeseries codec."},
        { "delta_compression",         std::make_shared<DataTypeUInt8>(), "The codec is for delta compression."},
        { "experimental",              std::make_shared<DataTypeUInt8>(), "The codec is experimental."},
        { "notes",              std::make_shared<DataTypeString>(), "Performance notes."},
    };
}

void StorageSystemCodecs::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    CompressionCodecFactory::instance().getMutableColumns(res_columns);
}



}
