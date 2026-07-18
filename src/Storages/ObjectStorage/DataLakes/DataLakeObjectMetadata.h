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

    /// When set, PREWHERE / row-level filter must not be pushed into the format reader
    /// but applied as fallback FilterTransforms after reading. Needed when a transform
    /// added by the data lake (e.g. DuckLake hive partition constants) changes a column
    /// the filter references: the format reader would otherwise evaluate the filter on
    /// default-filled values.
    bool force_post_read_filters = false;
};

}
