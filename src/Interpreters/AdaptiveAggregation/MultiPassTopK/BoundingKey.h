#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <base/bit_cast.h>

/// The bounding key of a multi-pass top-K aggregation (see `TopKBoundingKey`): its choice, its values, and the filters
/// of the read by its values.

namespace DB::MultiPassTopKImpl
{

/// The key that bounds the ranks of the groups of a query of several keys: one that the aggregation reads from a
/// column of the table as is, of a 32-bit or 64-bit integer type, which the passes take as the bits of a `UInt64`.
/// Each of its values holds all the rows of its groups. A key of the primary or the partition key is preferred, as the
/// passes then read only the granules of its values. The filters by its values must merge into the `PREWHERE` of the
/// read, which the passes over some of its values need.
///
/// The bounding mode adds a pass that summarizes the rows of the values of the key in hash cells and evaluates the
/// filters over the whole input. It pays off when another key is wide, such as a `String`, so that the aggregation by
/// all the keys costs much more than the summary, and when the filters read only narrow columns.
std::optional<TopKBoundingKey> findBoundingKey(const AggregatingStep & aggregating, const MultiPassTopKInput & input);

/// Calls `f` with the data of a column of the bounding key in full, a column of 32-bit or 64-bit integers (see
/// `findBoundingKey`). The passes take a value of the key as its bits, which `bit_cast<UInt64>` gives.
template <typename F>
void withKeyData(const ColumnPtr & column, F && f)
{
    const auto full = column->convertToFullIfWrapped();
    [[maybe_unused]] const bool dispatched = castTypeToEither<ColumnUInt64, ColumnInt64, ColumnUInt32, ColumnInt32>(
        full.get(),
        [&](const auto & typed)
        {
            f(typed.getData());
            return true;
        });
    chassert(dispatched);
}

/// Calls `f(value, rows)` for every run of a repeated value of a column of the bounding key, with the value as its bits.
/// The rows of a value come in runs where the table is sorted by the key, and a run takes one call.
template <typename F>
void forEachKeyRun(const ColumnPtr & column, F && f)
{
    withKeyData(column, [&](const auto & data)
    {
        const size_t size = data.size();
        size_t begin = 0;
        while (begin < size)
        {
            size_t end = begin + 1;
            while (end < size && data[end] == data[begin])
                ++end;
            f(bit_cast<UInt64>(data[begin]), end - begin);
            begin = end;
        }
    });
}

/// Inserts into a column of the bounding key the value whose bits `withKeyData` gives.
void insertKeyValue(IColumn & column, UInt64 bits);

/// A filter of the read by the values of the bounding key: `dag` outputs the column of the key and `column`, a function
/// of the key and of constants.
struct TopKKeyFilter
{
    ActionsDAG dag;
    String column;
};

/// The set of `values` of the bounding key.
FutureSetPtr makeSet(const TopKBoundingKey & key, ColumnPtr values);

/// The filter `key IN set`.
TopKKeyFilter makeInFilter(const TopKBoundingKey & key, const FutureSetPtr & set, const ContextPtr & context);

/// The `PREWHERE` of a read with `filter` added to `existing` by a conjunction, or nullptr when a name of the new nodes
/// clashes with another node or a column of the read, which `findBoundingKey` rules out for the filters of its key.
PrewhereInfoPtr addToPrewhere(
    const PrewhereInfoPtr & existing, TopKKeyFilter filter, const TopKBoundingKey & key, const Block & read_header, const ContextPtr & context);

}
