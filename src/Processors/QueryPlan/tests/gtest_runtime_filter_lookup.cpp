#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

using namespace DB;

namespace
{

RuntimeFilterGeometry testGeometry()
{
    return RuntimeFilterGeometry{
        .exact_values_limit = 64,
        .exact_bytes_limit = 4096,
        .bloom_filter_bytes = 4096,
        .bloom_filter_hash_functions = 3,
        .pass_ratio_threshold_for_disabling = 1.0,
        .blocks_to_skip_before_reenabling = 0,
        .max_ratio_of_set_bits_in_bloom_filter = 1.0,
    };
}

UniqueRuntimeFilterPtr makePartial(size_t filters_to_merge)
{
    return std::make_unique<ApproximateRuntimeFilter>(
        filters_to_merge, std::make_shared<DataTypeUInt64>(), testGeometry(), /*distinct_keys_hint_=*/std::nullopt);
}

}

/// Parallel build streams register into the lookup one by one: the first registration installs a
/// filter that still expects merges from the remaining streams. Until the last stream arrives the
/// filter is not usable, and a concurrent `__applyFilter` in the same task (for example a scan's
/// PREWHERE running while another join builds its filter) must see no filter at all - looking up
/// a half-built filter would throw and fail the query.
TEST(RuntimeFilterLookup, HalfBuiltFilterIsInvisible)
{
    auto lookup = createRuntimeFilterLookup();

    /// `filters_to_merge` counts the merges still expected after this registration.
    lookup->add("key", "f", makePartial(/*filters_to_merge=*/1));
    EXPECT_EQ(lookup->find("key"), nullptr);

    /// The second (last) registration completes the filter; now it is visible and usable.
    lookup->add("key", "f", makePartial(/*filters_to_merge=*/0));
    auto filter = lookup->find("key");
    ASSERT_NE(filter, nullptr);

    auto column = ColumnUInt64::create();
    column->insertValue(42);
    ColumnWithTypeAndName values(std::move(column), std::make_shared<DataTypeUInt64>(), "x");
    EXPECT_NO_THROW(filter->find(values));
}
