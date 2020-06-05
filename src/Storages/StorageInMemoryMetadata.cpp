#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{

StorageInMemoryMetadata::StorageInMemoryMetadata(
    const ColumnsDescription & columns_,
    const IndicesDescription & secondary_indices_,
    const ConstraintsDescription & constraints_)
    : columns(columns_)
    , secondary_indices(secondary_indices_)
    , constraints(constraints_)
{
}
}
