
#include <Storages/System/StorageSystemCodecs.h>
#include <Common/SystemTableDocumentation.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypeString.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
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

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemCodecs) }

namespace DB
{

REGISTER_SYSTEM_TABLE_DOCUMENTATION(
    "codecs",
    .description = R"DOCS_MD(
Contains information about compression and encryption codecs.

You can use this table to get information about the available compression and encryption codecs
)DOCS_MD",
    .examples = R"DOCS_MD(
```sql title="Query"
SELECT * FROM system.codecs WHERE name='LZ4'
```

```text title="Response"
Row 1:
──────
name:                   LZ4
method_byte:            130
is_compression:         1
is_generic_compression: 1
is_encryption:          0
is_timeseries_codec:    0
is_experimental:        0
description:            Extremely fast; good compression; balanced speed and efficiency.
```
)DOCS_MD")

}
