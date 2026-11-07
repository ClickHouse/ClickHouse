#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <algorithm>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int TOO_MANY_PARTS;
}

namespace
{

/// Every key appears in the first `num_keys` rows, in a scrambled order, and once more after them.
ColumnPtr makeKeys(size_t num_keys)
{
    auto column = ColumnUInt64::create();
    for (size_t i = 0; i < 2 * num_keys; ++i)
        column->insertValue((i * 7919) % num_keys);
    return column;
}

template <typename SelectorType>
struct ScatterSelector
{
    PODArray<size_t> first_rows;
    SelectorType selector;
};

template <typename SelectorType>
ScatterSelector<SelectorType> buildSelector(const ColumnPtr & keys, size_t max_parts)
{
    ScatterSelector<SelectorType> result;
    buildScatterSelector(ColumnRawPtrs{keys.get()}, result.first_rows, result.selector, max_parts, getContext().context);
    return result;
}

/// The code and the message of the exception that `buildSelector` throws.
template <typename SelectorType>
std::pair<int, String> buildError(const ColumnPtr & keys, size_t max_parts)
{
    try
    {
        buildSelector<SelectorType>(keys, max_parts);
    }
    catch (const Exception & e)
    {
        return {e.code(), e.message()};
    }
    return {0, ""};
}

}

TEST(MergeTreeDataWriter, BuildScatterSelectorSmall)
{
    {
        auto keys = makeKeys(65536);
        auto ids16 = buildSelector<IColumn::SmallSelector>(keys, 0);
        auto ids64 = buildSelector<IColumn::Selector>(keys, 0);
        EXPECT_EQ(ids16.first_rows.size(), 65536uz);
        EXPECT_EQ(ids16.first_rows, ids64.first_rows);
        ASSERT_EQ(ids16.selector.size(), keys->size());
        EXPECT_TRUE(std::ranges::equal(ids16.selector, ids64.selector));
    }

    {
        /// 16-bit ids cannot name 70000 partitions: the selector stays empty, but `first_rows` lists every partition.
        auto keys = makeKeys(70000);
        auto ids16 = buildSelector<IColumn::SmallSelector>(keys, 0);
        auto ids64 = buildSelector<IColumn::Selector>(keys, 0);
        EXPECT_EQ(ids16.first_rows.size(), 70000uz);
        EXPECT_EQ(ids16.first_rows, ids64.first_rows);
        EXPECT_TRUE(ids16.selector.empty());
    }

    {
        auto keys = makeKeys(70000);
        for (size_t max_parts : {1uz, 100uz, 65536uz})
        {
            SCOPED_TRACE(max_parts);
            auto error = buildError<IColumn::SmallSelector>(keys, max_parts);
            EXPECT_EQ(error.first, ErrorCodes::TOO_MANY_PARTS);
            EXPECT_EQ(error, buildError<IColumn::Selector>(keys, max_parts));
        }
    }
}
