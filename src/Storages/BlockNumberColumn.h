#pragma once
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Compression/CompressionFactory.h>

namespace DB
{

struct BlockNumberColumn
{
    static const String name;
    static const DataTypePtr type;
    static const CompressionCodecPtr compression_codec;
};

}
