#pragma once

#include <Common/COW.h>

#include <base/defines.h>
#include <memory>
#include <vector>

namespace DB
{

/// A range of column values between row indexes `from` and `to`. The name "equal range" is due to table sorting as its main use case: With
/// a PRIMARY KEY (c_pk1, c_pk2, ...), the first PK column is fully sorted. The second PK column is sorted within equal-value runs of the
/// first PK column, and so on. The number of runs (ranges) per column increases from one primary key column to the next. An "equal range"
/// is a run in a previous column, within the values of the current column can be sorted.
struct EqualRange
{
    size_t from;   /// inclusive
    size_t to;     /// exclusive
    EqualRange(size_t from_, size_t to_) : from(from_), to(to_) { chassert(from <= to); }
    size_t size() const { return to - from; }
};

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
