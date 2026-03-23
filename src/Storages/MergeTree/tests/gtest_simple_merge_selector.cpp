#include <gtest/gtest.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>

#include <fmt/format.h>

#include <iostream>

using namespace DB;

TEST(SimpleMergeSelector, TestRowsConstraint)
{
    SimpleMergeSelector::Settings settings;
    settings.base = 2.0;
    SimpleMergeSelector selector(settings);
    std::vector<std::string> part_names = {"all_0_0_0", "all_1_1_0", "all_2_2_0"};
    PartsRange parts_range;

    for (const auto & part_name : part_names)
    {
        parts_range.push_back(PartProperties
        {
            .name = part_name,
            .info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
            .size = 10 * 1024,
            .age = 0,
            .rows = 100,
        });
    }

    size_t max_bytes = 100 * 1024 * 1024;

    {
        size_t max_rows = 1000;
        std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};
        auto selected = selector.select({parts_range}, constraints, nullptr);

        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 3);
    }

    {
        size_t max_rows = 250;
        std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};
        auto selected = selector.select({parts_range}, constraints, nullptr);

        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 2);
    }

    {
        size_t max_rows = 50;
        std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};
        auto selected = selector.select({parts_range}, constraints, nullptr);

        ASSERT_EQ(selected.size(), 0);
    }
}


/// Simulation parameters for testing merge selector behavior under rapid small part insertion.
struct SimulationResult
{
    size_t total_bytes_written = 0;   /// Total bytes written by merges (write amplification numerator)
    size_t total_source_bytes = 0;    /// Total bytes from original inserts
    size_t num_merges = 0;            /// Number of merge operations performed
    size_t max_parts = 0;             /// Maximum number of parts observed at any point
    size_t final_parts = 0;           /// Number of parts at end of simulation

    double writeAmplification() const
    {
        return static_cast<double>(total_bytes_written + total_source_bytes) / static_cast<double>(total_source_bytes);
    }
};

/// Simulate rapid small part insertion and merging.
///
/// Model: every `insert_interval_sec` seconds, a new part of `part_size` bytes is inserted.
/// After each insert, we run merge selection; if a merge is selected, we execute it
/// (replace the range with a single merged part, age = 0). Time advances by
/// max(insert_interval, merge_time), where merge_time = sum_size / merge_speed.
/// After all inserts, we keep running merge selection (with time advancing) until
/// no more merges are possible or 60 days pass.
static SimulationResult runSimulation(
    const SimpleMergeSelector::Settings & settings,
    size_t num_inserts,
    size_t part_size,
    size_t part_rows,
    time_t insert_interval_sec,
    size_t merge_speed_bytes_per_sec = 100 * 1024 * 1024)
{
    SimulationResult result;

    PartsRange parts;
    int64_t next_block = 0;

    const size_t max_bytes = 100ULL * 1024 * 1024 * 1024;
    const size_t max_rows = std::numeric_limits<size_t>::max();
    std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};

    auto age_all_parts = [&](time_t delta)
    {
        PartsRange aged;
        aged.reserve(parts.size());
        for (auto & p : parts)
        {
            aged.push_back(PartProperties{
                .name = p.name,
                .info = p.info,
                .size = p.size,
                .age = p.age + delta,
                .rows = p.rows,
            });
        }
        parts.swap(aged);
    };

    auto try_merge = [&]() -> bool
    {
        SimpleMergeSelector selector(settings);
        PartsRanges selected = selector.select({parts}, constraints, nullptr);
        if (selected.empty())
            return false;

        const auto & sel = selected[0];
        /// Find the range in parts by matching names
        size_t merge_begin = 0;
        size_t merge_end = 0;
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (parts[i].name == sel.front().name)
                merge_begin = i;
            if (parts[i].name == sel.back().name)
            {
                merge_end = i + 1;
                break;
            }
        }

        size_t sum_size = 0;
        size_t sum_rows = 0;
        int64_t min_block = parts[merge_begin].info.min_block;
        int64_t max_block = parts[merge_begin].info.max_block;
        uint32_t max_level = parts[merge_begin].info.level;
        for (size_t i = merge_begin; i < merge_end; ++i)
        {
            sum_size += parts[i].size;
            sum_rows += parts[i].rows;
            min_block = std::min(min_block, parts[i].info.min_block);
            max_block = std::max(max_block, parts[i].info.max_block);
            max_level = std::max(max_level, parts[i].info.level);
        }

        result.total_bytes_written += sum_size;
        result.num_merges++;

        auto merged_name = fmt::format("all_{}_{}_{}", min_block, max_block, max_level + 1);
        auto merged_info = MergeTreePartInfo::fromPartName(merged_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);

        /// Calculate merge time and advance ages
        time_t merge_time = std::max<time_t>(1, static_cast<time_t>(sum_size / merge_speed_bytes_per_sec));
        age_all_parts(merge_time);

        PartsRange new_parts;
        new_parts.reserve(parts.size() - (merge_end - merge_begin) + 1);
        for (size_t i = 0; i < merge_begin; ++i)
            new_parts.push_back(std::move(parts[i]));

        new_parts.push_back(PartProperties{
            .name = merged_name,
            .info = merged_info,
            .size = sum_size,
            .age = 0,
            .rows = sum_rows,
        });

        for (size_t i = merge_end; i < parts.size(); ++i)
            new_parts.push_back(std::move(parts[i]));

        parts.swap(new_parts);
        return true;
    };

    /// Phase 1: Insert parts and run merges
    for (size_t i = 0; i < num_inserts; ++i)
    {
        auto name = fmt::format("all_{0}_{0}_0", next_block);
        auto info = MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
        parts.push_back(PartProperties{
            .name = name,
            .info = info,
            .size = part_size,
            .age = 0,
            .rows = part_rows,
        });
        next_block++;
        result.total_source_bytes += part_size;

        /// Advance time by insert interval
        age_all_parts(insert_interval_sec);

        result.max_parts = std::max(result.max_parts, parts.size());

        /// Try to merge after each insert
        while (try_merge())
        {
            result.max_parts = std::max(result.max_parts, parts.size());
        }
    }

    /// Phase 2: Let time pass and keep merging until stable
    for (time_t extra = 0; extra < 60 * 86400; extra += 60)
    {
        age_all_parts(60);

        bool merged = false;
        while (try_merge())
        {
            merged = true;
            result.max_parts = std::max(result.max_parts, parts.size());
        }
        if (!merged && extra > 86400)
            break;
    }

    result.final_parts = parts.size();
    return result;
}


/// Test that small_parts_min_count reduces write amplification under rapid small part insertion.
///
/// Scenario: 200 small parts (1 MB each) inserted every 1 second.
/// Without small_parts_min_count: selector may merge 2-3 fresh parts at a time -> many small merges -> high write amplification.
/// With small_parts_min_count = 8: forces batching of fresh small parts -> fewer, larger merges.
TEST(SimpleMergeSelector, SmallPartsMinCountReducesWriteAmplification)
{
    const size_t num_inserts = 200;
    const size_t part_size = 1 * 1024 * 1024;       /// 1 MB
    const size_t part_rows = 10000;
    const time_t insert_interval = 1;                /// 1 second between inserts

    /// Baseline: no small parts restriction (small_parts_min_count = 0 means disabled)
    SimpleMergeSelector::Settings baseline_settings;
    baseline_settings.base = 5;
    auto baseline = runSimulation(baseline_settings, num_inserts, part_size, part_rows, insert_interval);

    /// With small parts restriction: require at least 8 parts when all parts are small (< 10 MB) and fresh (< 600 s)
    SimpleMergeSelector::Settings small_settings;
    small_settings.base = 5;
    small_settings.small_parts_threshold = 10 * 1024 * 1024;  /// 10 MB
    small_settings.small_parts_min_count = 8;
    small_settings.small_parts_max_age = 600;                  /// 10 minutes
    auto with_small = runSimulation(small_settings, num_inserts, part_size, part_rows, insert_interval);

    std::cerr << "=== Small Parts Min Count Simulation ===" << std::endl;
    std::cerr << "Baseline:        WA=" << baseline.writeAmplification()
              << " merges=" << baseline.num_merges
              << " max_parts=" << baseline.max_parts
              << " final_parts=" << baseline.final_parts << std::endl;
    std::cerr << "With small_parts: WA=" << with_small.writeAmplification()
              << " merges=" << with_small.num_merges
              << " max_parts=" << with_small.max_parts
              << " final_parts=" << with_small.final_parts << std::endl;

    /// The new setting should reduce write amplification (or at least not increase it significantly)
    /// and reduce the number of merges
    EXPECT_LE(with_small.writeAmplification(), baseline.writeAmplification() + 0.5)
        << "Small parts restriction should not significantly increase write amplification";
    EXPECT_LE(with_small.num_merges, baseline.num_merges)
        << "Small parts restriction should reduce or maintain merge count";
}
