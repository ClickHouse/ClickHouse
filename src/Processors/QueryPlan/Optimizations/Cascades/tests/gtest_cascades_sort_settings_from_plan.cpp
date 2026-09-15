#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerContext.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

namespace
{

/// Every setting `SortingStep::Settings(const Settings &)` reads, at a value that differs from its
/// default and from every other value here, so a field left unmapped or crossed with its neighbour
/// cannot compare equal by accident.
void setDistinctSortSettings(Settings & settings)
{
    settings.set("max_block_size", 4096u);
    settings.set("max_rows_to_sort", 111u);
    settings.set("max_bytes_to_sort", 222u);
    settings.set("sort_overflow_mode", "break");
    settings.set("max_bytes_before_remerge_sort", 333u);
    settings.set("remerge_sort_lowered_memory_bytes_ratio", 0.25);
    settings.set("max_bytes_before_external_sort", 444u);
    settings.set("min_free_disk_space_for_temporary_data", 555u);
    settings.set("prefer_external_sort_block_bytes", 666u);
    settings.set("read_in_order_use_virtual_row", true);
    settings.set("read_in_order_use_virtual_row_per_block", true);
    settings.set("read_in_order_use_buffering", true);
    settings.set("temporary_files_codec", "ZSTD");
    settings.set("temporary_files_buffer_size", 777u);

    /// `max_bytes_in_query_before_external_sort` is derived from this ratio against the memory
    /// available when the sort is planned, so two derivations of the same ratio need not agree.
    /// Pinned to 0 to keep the comparison below a pure function of the settings.
    settings.set("max_bytes_ratio_before_external_sort", 0.);
}

}

/// A sort added by a Cascades enforcer takes its limits from the plan, so the plan must carry every
/// value the query's own sorts get from `Settings`. The two mappings live apart --
/// `SortingStep::Settings(const Settings &)` and `makeSortSettings` over
/// `QueryPlanOptimizationSettings` -- and nothing but this comparison links them, so a sort setting
/// added to one and not the other would silently default under Cascades.
TEST(CascadesSortSettings, PlanCarriesEverySortSetting)
{
    Settings settings;
    setDistinctSortSettings(settings);

    const SortingStep::Settings expected(settings);
    const QueryPlanOptimizationSettings optimization_settings(settings, 0, "", {}, nullptr, false);
    const SortingStep::Settings actual = makeSortSettings(optimization_settings);

    EXPECT_EQ(expected, actual);
}

/// With the ratio at zero the derived byte limit is zero on both sides, which is what lets the
/// comparison above use `operator==` over the whole struct.
TEST(CascadesSortSettings, ZeroSpillRatioDerivesZero)
{
    Settings settings;
    setDistinctSortSettings(settings);

    EXPECT_EQ(SortingStep::Settings(settings).max_bytes_in_query_before_external_sort, 0u);

    const QueryPlanOptimizationSettings optimization_settings(settings, 0, "", {}, nullptr, false);
    EXPECT_EQ(makeSortSettings(optimization_settings).max_bytes_in_query_before_external_sort, 0u);
}

/// A non-zero ratio is carried through unresolved, so the sort resolves it where it is planned.
TEST(CascadesSortSettings, SpillRatioIsCarriedUnresolved)
{
    Settings settings;
    settings.set("max_bytes_ratio_before_external_sort", 0.25);

    const QueryPlanOptimizationSettings optimization_settings(settings, 0, "", {}, nullptr, false);
    EXPECT_DOUBLE_EQ(makeSortSettings(optimization_settings).max_bytes_ratio_before_external_sort, 0.25);
}

/// `read_in_order_use_virtual_row_per_block` is the conjunction of two settings, so it is off unless
/// both are on. The equality above cannot see that: it sets both on, where a mapping that dropped
/// either conjunct still yields the same value. Both one-sided cases are pinned, because a mapping
/// that kept only one conjunct is visible to that case alone.
TEST(CascadesSortSettings, VirtualRowPerBlockNeedsBothSettings)
{
    Settings settings;
    settings.set("read_in_order_use_virtual_row", false);
    settings.set("read_in_order_use_virtual_row_per_block", true);

    EXPECT_FALSE(SortingStep::Settings(settings).read_in_order_use_virtual_row_per_block);

    const QueryPlanOptimizationSettings optimization_settings(settings, 0, "", {}, nullptr, false);
    EXPECT_FALSE(makeSortSettings(optimization_settings).read_in_order_use_virtual_row_per_block);

    Settings reversed;
    reversed.set("read_in_order_use_virtual_row", true);
    reversed.set("read_in_order_use_virtual_row_per_block", false);

    EXPECT_FALSE(SortingStep::Settings(reversed).read_in_order_use_virtual_row_per_block);

    const QueryPlanOptimizationSettings reversed_optimization_settings(reversed, 0, "", {}, nullptr, false);
    EXPECT_FALSE(makeSortSettings(reversed_optimization_settings).read_in_order_use_virtual_row_per_block);
}

/// Resolving the ratio rejects an out-of-range value, and it must be rejected on the Cascades path
/// too. The range is checked before any memory is read, so both sides throw the same way whatever
/// the machine reports. This is also what pins the resolution itself to the plan mapping: with the
/// ratio at zero both sides resolve to zero, so only an out-of-range value can tell them apart.
TEST(CascadesSortSettings, OutOfRangeSpillRatioIsRejectedOnBothPaths)
{
    Settings settings;
    settings.set("max_bytes_ratio_before_external_sort", 1.5);

    EXPECT_THROW({ const SortingStep::Settings rejected(settings); }, Exception);

    const QueryPlanOptimizationSettings optimization_settings(settings, 0, "", {}, nullptr, false);
    EXPECT_THROW(makeSortSettings(optimization_settings), Exception);
}
