#pragma once
#include <Interpreters/ActionsDAG.h>

#include <optional>

namespace DB
{
template <typename T, UInt8 small_set_size>
class RoaringBitmapWithSmallSet;

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> schema_transform;

    using ExcludedRows = RoaringBitmapWithSmallSet<size_t, 32>;
    using ExcludedRowsPtr = std::shared_ptr<ExcludedRows>;

    /// Excluded rows indexes from selection vector
    ExcludedRowsPtr excluded_rows;
};

/// True when a deletion vector (or similar) will filter rows via DeletionVectorTransform.
/// Count-from-files cache must skip these objects: the cache key is data-file identity only,
/// while excluded_rows can change independently (Iceberg puffin DVs, DeltaLake selection vectors).
/// Cluster task serialization must also fail closed when these cannot be carried on the wire.
bool hasNonEmptyExcludedRows(const DataLakeObjectMetadata & metadata);
bool hasNonEmptyExcludedRows(const std::optional<DataLakeObjectMetadata> & metadata);

}
