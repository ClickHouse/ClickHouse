#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <base/scope_guard.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

/// `transform(Chunk &)` is `protected` in the concrete transform but `public` in the
/// abstract `ISimpleTransform` base, so the test drives it through the base reference,
/// which virtual-dispatches to the override (exactly what the pipeline executor does).
void runTransform(ISimpleTransform & transform, Chunk & chunk)
{
    transform.transform(chunk);
}

SharedHeader makeHeader()
{
    Block header{
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "key"),
        ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "value"),
    };
    return std::make_shared<const Block>(std::move(header));
}

/// One chunk with `count` rows where every row has a distinct grouping key starting at
/// `first_key`. Distinct keys force a new run (and therefore a pushed output slice) on
/// every row boundary, so `output_slices` is non-empty well before the chunk is fully
/// consumed.
Chunk makeDistinctKeyChunk(UInt64 first_key, UInt64 count)
{
    auto key_column = ColumnUInt64::create();
    auto value_column = ColumnUInt64::create();
    for (UInt64 i = 0; i < count; ++i)
    {
        key_column->insertValue(first_key + i);
        value_column->insertValue(i);
    }
    return Chunk(Columns{std::move(key_column), std::move(value_column)}, count);
}

/// One sorted chunk of `num_groups` contiguous groups, each `group_size` rows, with ascending
/// keys `first_key, first_key + 1, ...`. This is valid sorted-stream input. Under `LIMIT 1 BY
/// key` only the first row of each group survives, so the kept rows are sparse across a large
/// chunk: `LimitBySortedStreamTransform::transform` populates all slices in the run loop and
/// then reaches `materializeSlicesIntoChunk`, whose mask allocation (sized to the whole chunk)
/// is the after-slice throw point. Keep `num_groups` small so `output_slices` itself never
/// trips the limit during the loop; keep `group_size` large so the mask allocation does.
Chunk makeSortedGroupedChunk(UInt64 first_key, UInt64 num_groups, UInt64 group_size)
{
    auto key_column = ColumnUInt64::create();
    auto value_column = ColumnUInt64::create();
    for (UInt64 g = 0; g < num_groups; ++g)
        for (UInt64 i = 0; i < group_size; ++i)
        {
            key_column->insertValue(first_key + g);
            value_column->insertValue(i);
        }
    const UInt64 count = num_groups * group_size;
    return Chunk(Columns{std::move(key_column), std::move(value_column)}, count);
}

}

/// Regression test for the AST fuzzer finding "Logical error: 'output_slices.empty()'"
/// (STID 2508-2fed). `output_slices` is a member scratch buffer that was cleared only on
/// the success path. When `transform` threw after populating it (for example
/// MEMORY_LIMIT_EXCEEDED while the grouping hash table grows), `ISimpleTransform::work`
/// kept the transform alive and could call `transform` again on the next chunk, where the
/// stale slices corrupted the next result (and tripped a `chassert`). The fix clears the
/// buffer at the start of `transform`.
TEST(LimitByTransform, ClearsOutputSlicesWhenTransformThrows)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;
    const Int64 prev_hard_limit = thread_tracker.getHardLimit();

    CurrentThread::flushUntrackedMemory();
    SCOPE_EXIT({
        thread_tracker.setHardLimit(prev_hard_limit);
        CurrentThread::flushUntrackedMemory();
    });

    /// `LIMIT 1 BY key` over the non-constant "key" column -> hash-map variant.
    LimitByTransform transform(makeHeader(), /*group_length_=*/ 1, /*group_offset_=*/ 0, Names{"key"});

    /// First chunk: many distinct groups. Clamp the hard limit just above the current
    /// usage so that growing the grouping hash table overshoots and throws part way
    /// through, after at least one output slice has already been pushed.
    Chunk first_chunk = makeDistinctKeyChunk(/*first_key=*/ 0, /*count=*/ 500000);

    CurrentThread::flushUntrackedMemory();
    thread_tracker.setHardLimit(thread_tracker.get() + 64 * 1024);

    bool threw = false;
    try
    {
        runTransform(transform, first_chunk);
    }
    catch (const Exception & e)
    {
        threw = (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    /// Lift the limit before doing anything else so cleanup and the second chunk run freely.
    thread_tracker.setHardLimit(prev_hard_limit);
    CurrentThread::flushUntrackedMemory();

    ASSERT_TRUE(threw) << "expected the first chunk to hit the memory limit mid-transform";

    /// Re-enter `transform` exactly as the pipeline executor would after catching the
    /// exception. Before the fix the leaked slices from the first chunk corrupted this call
    /// (out-of-range reuse, tripping a `chassert` in debug builds). Use keys outside the
    /// first chunk's range so the result is independent of how far the first chunk got.
    Chunk second_chunk = makeDistinctKeyChunk(/*first_key=*/ 1000000000, /*count=*/ 4);
    ASSERT_NO_THROW(runTransform(transform, second_chunk));

    /// Every distinct key survives `LIMIT 1 BY key`, and the output columns must agree on
    /// the row count (a leaked slice would corrupt this).
    EXPECT_EQ(second_chunk.getNumRows(), 4u);
    for (const auto & column : second_chunk.getColumns())
        EXPECT_EQ(column->size(), second_chunk.getNumRows());
}

/// Same regression but for the sorted-stream variant `LimitBySortedStreamTransform`, which
/// has its own `output_slices` scratch buffer and its own after-slice throw point: the
/// chunk-sized filter mask allocated inside `materializeSlicesIntoChunk` once the slices are
/// populated. Without the matching clear in `LimitBySortedStreamTransform::transform`,
/// re-entry on the next chunk reuses the stale slices.
TEST(LimitBySortedStreamTransform, ClearsOutputSlicesWhenTransformThrows)
{
    MainThreadStatus::getInstance();
    auto & thread_tracker = CurrentThread::get().memory_tracker;
    const Int64 prev_hard_limit = thread_tracker.getHardLimit();

    CurrentThread::flushUntrackedMemory();
    SCOPE_EXIT({
        thread_tracker.setHardLimit(prev_hard_limit);
        CurrentThread::flushUntrackedMemory();
    });

    /// Sorted-stream `LIMIT 1 BY key` over the non-constant "key" column.
    SortDescription sort_description;
    sort_description.push_back(SortColumnDescription("key"));
    LimitBySortedStreamTransform transform(makeHeader(), /*group_length_=*/ 1, /*group_offset_=*/ 0, sort_description);

    /// First chunk: a few large contiguous groups (already sorted by key). The sorted-stream
    /// variant has no grouping hash table, so its after-slice throw point is the chunk-sized
    /// filter mask inside `materializeSlicesIntoChunk`. `LIMIT 1 BY key` keeps only each
    /// group's first row, so survival is sparse and that mask (one byte per source row) is
    /// allocated after every slice has been pushed. Clamp the hard limit just above current
    /// usage so that mask allocation overshoots and throws.
    Chunk first_chunk = makeSortedGroupedChunk(/*first_key=*/ 0, /*num_groups=*/ 4, /*group_size=*/ 2000000);

    CurrentThread::flushUntrackedMemory();
    thread_tracker.setHardLimit(thread_tracker.get() + 64 * 1024);

    bool threw = false;
    try
    {
        runTransform(transform, first_chunk);
    }
    catch (const Exception & e)
    {
        threw = (e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED);
    }

    /// Lift the limit before doing anything else so cleanup and the second chunk run freely.
    thread_tracker.setHardLimit(prev_hard_limit);
    CurrentThread::flushUntrackedMemory();

    ASSERT_TRUE(threw) << "expected the first chunk to hit the memory limit mid-transform";

    /// Re-enter `transform` exactly as the pipeline executor would after catching the
    /// exception. Before the fix the leaked slices from the first chunk corrupted this call
    /// (out-of-range reuse, tripping a `chassert` in debug builds). Keys ascend past the
    /// first chunk's range so the second chunk stays valid sorted-stream input.
    Chunk second_chunk = makeDistinctKeyChunk(/*first_key=*/ 1000000000, /*count=*/ 4);
    ASSERT_NO_THROW(runTransform(transform, second_chunk));

    /// Every distinct key survives `LIMIT 1 BY key`, and the output columns must agree on
    /// the row count (a leaked slice would corrupt this).
    EXPECT_EQ(second_chunk.getNumRows(), 4u);
    for (const auto & column : second_chunk.getColumns())
        EXPECT_EQ(column->size(), second_chunk.getNumRows());
}
