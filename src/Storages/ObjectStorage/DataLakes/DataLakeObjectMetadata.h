#pragma once

#include <base/types.h>

#include <memory>
#include <optional>

namespace DB
{

class ActionsDAG;

template <typename T, UInt8 small_set_size>
class RoaringBitmapWithSmallSet;

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> schema_transform;

    using ExcludedRows = RoaringBitmapWithSmallSet<size_t, 32>;
    using ExcludedRowsPtr = std::shared_ptr<ExcludedRows>;

    /// Excluded rows indexes from selection vector
    ExcludedRowsPtr excluded_rows;

    /// The exact number of rows of the object as the table metadata records it (before excluding `excluded_rows`), if known
    /// (`numRecords` of Delta Lake). A query that reads nothing from the object takes its number of rows from here.
    std::optional<UInt64> num_rows;
};

}
