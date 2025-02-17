#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

DeltaLakeMetadata::DeltaLakeMetadata(
    ObjectStoragePtr,
    ConfigurationObserverPtr configuration_,
    ContextPtr)
    : log(getLogger("DeltaLakeMetadata"))
    , table_snapshot(
        std::make_shared<DeltaLake::TableSnapshot>(getKernelHelper(configuration_.lock()), log))
{
}

Strings DeltaLakeMetadata::getDataFiles() const
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "getDataFiles() is not implemented, you should use iterate() method instead");
}

ObjectIterator DeltaLakeMetadata::iterate() const
{
    return table_snapshot->iterate();
}

NamesAndTypesList DeltaLakeMetadata::getTableSchema() const
{
    return table_snapshot->getTableSchema();
}

NamesAndTypesList DeltaLakeMetadata::getReadSchema() const
{
    auto schema = table_snapshot->getReadSchema();
    auto partition_columns = table_snapshot->getPartitionColumns();
    if (!partition_columns.empty())
    {
        auto table_schema = getTableSchema();
        for (const auto & column : partition_columns)
        {
            auto name_and_type = table_schema.tryGetByName(column);
            if (name_and_type.has_value())
                schema.insert(schema.end(), name_and_type.value());
        }
    }
    return schema;
}

}

#endif
