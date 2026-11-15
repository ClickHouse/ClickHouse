#pragma once

#include <base/types.h>

#include <memory>
#include <optional>

namespace DB
{

class ActionsDAG;
class DeletionVectorBitmap;

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> schema_transform;

    using ExcludedRows = DeletionVectorBitmap;
    using ExcludedRowsPtr = std::shared_ptr<ExcludedRows>;

    /// Excluded rows indexes from selection vector
    ExcludedRowsPtr excluded_rows;

    /// The number of rows of the object as the table metadata records it (before excluding `excluded_rows`), if known.
    std::optional<UInt64> num_rows;
};

}
