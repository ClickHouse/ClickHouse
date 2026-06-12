#pragma once

#include <Common/COW.h>

#include <memory>
#include <vector>

namespace DB
{

struct EqualRange;
using EqualRanges = std::vector<EqualRange>;

struct ColumnCheckpoint;
using ColumnCheckpointPtr = std::shared_ptr<ColumnCheckpoint>;
using ColumnCheckpoints = std::vector<ColumnCheckpointPtr>;

class IColumn;

void intrusive_ptr_add_ref(const IColumn * c);
void intrusive_ptr_release(const IColumn * c);

using ColumnPtr = COW<IColumn>::Ptr;
using MutableColumnPtr = COW<IColumn>::MutablePtr;
using Columns = std::vector<ColumnPtr>;
using MutableColumns = std::vector<MutableColumnPtr>;

using ColumnRawPtrs = std::vector<const IColumn *>;

}
