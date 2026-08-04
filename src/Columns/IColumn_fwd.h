#pragma once

#include <Common/COW.h>
#include <Common/VectorWithMemoryTracking.h>

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
class ColumnConst;

void intrusive_ptr_add_ref(const IColumn * c);
void intrusive_ptr_release(const IColumn * c);

/// ColumnConst-specific overloads so that ColumnConstPtr/MutableColumnConstPtr can be
/// destroyed in TUs that only have a forward declaration of ColumnConst.
/// Defined in ColumnConst.cpp and forwarded to the IColumn versions.
void intrusive_ptr_add_ref(const ColumnConst * c);
void intrusive_ptr_release(const ColumnConst * c);

using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;
using ColumnConstPtr = COW<IColumn>::immutable_ptr<ColumnConst>;
using MutableColumnConstPtr = COW<IColumn>::mutable_ptr<ColumnConst>;
using Columns = std::vector<ColumnPtr>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using MutableColumns = std::vector<MutableColumnPtr>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
using ColumnRawPtrs = std::vector<const IColumn *>; // STYLE_CHECK_ALLOW_STD_CONTAINERS
}
