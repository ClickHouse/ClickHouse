#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnFixedString.h>

#include <gtest/gtest.h>
#include <base/types.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/scope_guard_safe.h>

using namespace DB;

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}
}

TEST(ColumnTuple, EmptyTupleIndex)
{
    ColumnPtr tuple_col = ColumnTuple::create(5);

    auto indexes = ColumnUInt64::create();
    auto & data = indexes->getData();
    data.push_back(0);
    data.push_back(1);
    data.push_back(2);

    ColumnPtr res_lim_zero = tuple_col->index(*indexes, 0);

    ASSERT_EQ(res_lim_zero->size(), indexes->size());

    ColumnPtr res_lim_non_zero = tuple_col->index(*indexes, 1);

    ASSERT_EQ(res_lim_non_zero->size(), 1);

    try
    {
        (void)tuple_col->index(*indexes, 10);
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}

TEST(ColumnTuple, EmptyTupleExpand)
{
    ColumnPtr tuple_col = ColumnTuple::create(5);

    IColumn::Filter mask;
    mask.push_back(true);
    mask.push_back(false);
    mask.push_back(true);
    mask.push_back(false);
    mask.push_back(true);

    ColumnPtr filtered = tuple_col->filter(mask, -1);
    ASSERT_EQ(filtered->size(), 3);

    auto expanded = IColumn::mutate(filtered);
    expanded->expand(mask, false);

    ASSERT_EQ(expanded->size(), mask.size());

    IColumn::Filter inverted_mask;
    inverted_mask.push_back(false);
    inverted_mask.push_back(true);
    inverted_mask.push_back(false);
    inverted_mask.push_back(true);
    inverted_mask.push_back(false);

    ColumnPtr filtered_inv = tuple_col->filter(inverted_mask, -1);
    ASSERT_EQ(filtered_inv->size(), 2);

    auto expanded_inv = IColumn::mutate(filtered_inv);
    expanded_inv->expand(mask, true);

    ASSERT_EQ(expanded_inv->size(), mask.size());
}

TEST(ColumnTuple, InsertDefaultIsExceptionSafe)
{
    /// First element is plain: its insertDefault succeeds.
    /// Second element is a wide FixedString: its insertDefault triggers a large allocation
    /// that overshoots a clamped memory limit and throws MEMORY_LIMIT_EXCEEDED - the same
    /// way a real query hits a memory limit in the middle of a default insert.
    static constexpr size_t huge = 64 * 1024 * 1024;

    auto first = ColumnFloat64::create();
    first->reserve(1);
    MutableColumns elements;
    elements.push_back(std::move(first));
    elements.push_back(ColumnFixedString::create(huge));
    auto tuple = ColumnTuple::create(std::move(elements));

    {
        ThreadStatus thread_status;
        const Int64 prev_hard_limit = total_memory_tracker.getHardLimit();
        SCOPE_EXIT_SAFE(total_memory_tracker.setHardLimit(prev_hard_limit));
        total_memory_tracker.setHardLimit(total_memory_tracker.get() + 1024);

        ASSERT_THROW(tuple->insertDefault(), Exception);
    }

    /// The first element must have been rolled back so that all nested columns stay in sync,
    /// otherwise a later popBack would over-pop the shorter element.
    const auto & tuple_concrete = assert_cast<const ColumnTuple &>(*tuple);
    ASSERT_EQ(tuple_concrete.getColumn(0).size(), 0);
    ASSERT_EQ(tuple_concrete.getColumn(1).size(), 0);
    ASSERT_EQ(tuple->size(), 0);
}

TEST(ColumnTuple, EmptyTuplePermute)
{
    ColumnPtr tuple_col = ColumnTuple::create(5);

    IColumn::Permutation perm;
    perm.push_back(0);
    perm.push_back(1);
    perm.push_back(2);

    ColumnPtr res_ok = tuple_col->permute(perm, 2);
    ASSERT_EQ(res_ok->size(), 2);

    IColumn::Permutation short_perm;
    short_perm.push_back(0);
    short_perm.push_back(1);

    try
    {
        (void)tuple_col->permute(short_perm, 3);
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}
