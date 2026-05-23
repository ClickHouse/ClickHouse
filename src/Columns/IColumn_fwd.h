#pragma once

#include <Common/COW.h>
#include <Common/PODArray_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Types.h>

#include <memory>
#include <vector>

namespace DB
{

struct EqualRange;
using EqualRanges = VectorWithMemoryTracking<EqualRange>;

struct ColumnCheckpoint;
using ColumnCheckpointPtr = std::shared_ptr<ColumnCheckpoint>;
using ColumnCheckpoints = VectorWithMemoryTracking<ColumnCheckpointPtr>;

class IColumn;

/// Forward-declared alias of `IColumn::Filter` for use in headers that only
/// forward-declare `IColumn`. Must stay in sync with the definition in `IColumn.h`.
using IColumnFilter = PaddedPODArray<UInt8>;

void intrusive_ptr_add_ref(const IColumn * c);
void intrusive_ptr_release(const IColumn * c);

using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;
using Columns = std::vector<ColumnPtr>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using MutableColumns = std::vector<MutableColumnPtr>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using ColumnRawPtrs = std::vector<const IColumn *>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
}
