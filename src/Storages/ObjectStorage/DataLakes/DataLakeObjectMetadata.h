#pragma once
#include <Interpreters/ActionsDAG.h>

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

    /// The number of rows of the object as the table metadata records it (before excluding `excluded_rows`), if known.
    std::optional<UInt64> num_rows;
};

}
