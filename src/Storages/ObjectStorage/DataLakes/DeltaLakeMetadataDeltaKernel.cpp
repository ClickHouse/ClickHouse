#include "config.h"

#if USE_PARQUET && USE_DELTA_KERNEL_RS
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

namespace DB
{

DeltaLakeMetadataDeltaKernel::DeltaLakeMetadataDeltaKernel(
    ObjectStoragePtr object_storage,
    ConfigurationObserverPtr configuration_,
    ContextPtr)
    : log(getLogger("DeltaLakeMetadata"))
    , table_snapshot(
        std::make_shared<DeltaLake::TableSnapshot>(getKernelHelper(configuration_.lock()), object_storage, log))
{
}

bool DeltaLakeMetadataDeltaKernel::operator ==(const IDataLakeMetadata & metadata) const
{
    const auto & delta_lake_metadata = dynamic_cast<const DeltaLakeMetadataDeltaKernel &>(metadata);
    return table_snapshot->getVersion() == delta_lake_metadata.table_snapshot->getVersion();
}

bool DeltaLakeMetadataDeltaKernel::update(const ContextPtr &)
{
    return table_snapshot->update();
}

Strings DeltaLakeMetadataDeltaKernel::getDataFiles() const
{
    throwNotImplemented("getDataFiles()");
}

ObjectIterator DeltaLakeMetadataDeltaKernel::iterate() const
{
    return table_snapshot->iterate();
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getTableSchema() const
{
    return table_snapshot->getTableSchema();
}

NamesAndTypesList DeltaLakeMetadataDeltaKernel::getReadSchema() const
{
    auto schema = table_snapshot->getReadSchema();

    /// Read schema does not contain partition columns
    /// because they are not present in the actual data.
    /// We have to add them here.
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

size_t DeltaLakeMetadataDeltaKernel::getMemoryBytes() const
{
    return sizeof(*this) + sizeof(DeltaLake::TableSnapshot);
}

}

#endif
