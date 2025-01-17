#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>
#include "config.h"

#if USE_PARQUET
#include <Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.h>

namespace DB
{

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
    return table_snapshot->getSchema();
}

}

#endif
