#pragma once
#include <Interpreters/ActionsDAG.h>

namespace DB
{
class DeletionVectorBitmap;

struct DataLakeObjectMetadata
{
    std::shared_ptr<ActionsDAG> schema_transform;

    using ExcludedRows = DeletionVectorBitmap;
    using ExcludedRowsPtr = std::shared_ptr<ExcludedRows>;

    /// Excluded rows indexes from selection vector
    ExcludedRowsPtr excluded_rows;
};

}
