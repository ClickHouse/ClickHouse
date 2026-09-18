#pragma once
#include <Interpreters/ActionsDAG.h>
#include <Core/Field.h>

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

}
