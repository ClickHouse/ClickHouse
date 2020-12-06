#pragma once

#include <Storages/MergeTree/MergeSelector.h>


namespace DB
{

class SimpleMergeSelector final : public IMergeSelector
{
public:
    struct Settings
    {
        /// Zero means unlimited.
        size_t max_parts_to_merge_at_once = 100;

        /** Minimum ratio of size of one part to all parts in set of parts to merge (for usual cases).
          * For example, if all parts have equal size, it means, that at least 'base' number of parts should be merged.
          * If parts has non-uniform sizes, then minimum number of parts to merge is effectively increased.
          * This behaviour balances merge-tree workload.
          * It called 'base', because merge-tree depth could be estimated as logarithm with that base.
          *
          * If base is higher - then tree gets more wide and narrow, lowering write amplification.
          * If base is lower - then merges occurs more frequently, lowering number of parts in average.
          *
          * We need some balance between write amplification and number of parts.
          */
        double base = 5;

        /** Base is lowered until 1 (effectively means "merge any two parts") depending on several variables:
          *
          * 1. Total number of parts in partition. If too many - then base is lowered.
          * It means: when too many parts - do merges more urgently.
          *
          * 2. Minimum age of parts participating in merge. If higher age - then base is lowered.
          * It means: do less wide merges only rarely.
          *
          * 3. Sum size of parts participating in merge. If higher - then more age is required to lower base. So, base is lowered slower.
          * It means: for small parts, it's worth to merge faster, even not so wide or balanced.
          *
          * We have multivariative dependency. Let it be logarithmic of size and somewhat multi-linear by other variables,
          *  between some boundary points, and constant outside.
          */

        size_t min_size_to_lower_base = 1024 * 1024;
        size_t max_size_to_lower_base = 100ULL * 1024 * 1024 * 1024;

        time_t min_age_to_lower_base_at_min_size = 10;
        time_t min_age_to_lower_base_at_max_size = 10;
        time_t max_age_to_lower_base_at_min_size = 3600;
        time_t max_age_to_lower_base_at_max_size = 30 * 86400;

        size_t min_parts_to_lower_base = 10;
        size_t max_parts_to_lower_base = 50;

        /// Add this to size before all calculations. It means: merging even very small parts has it's fixed cost.
        size_t size_fixed_cost_to_add = 5 * 1024 * 1024;

        /** Heuristic:
          * Make some preference for ranges, that sum_size is like (in terms of ratio) to part previous at left.
          */
        bool enable_heuristic_to_align_parts = true;
        double heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part = 0.9;
        double heuristic_to_align_parts_max_absolute_difference_in_powers_of_two = 0.5;
        double heuristic_to_align_parts_max_score_adjustment = 0.75;

        /** Heuristic:
          * From right side of range, remove all parts, that size is less than specified ratio of sum_size.
          */
        bool enable_heuristic_to_remove_small_parts_at_right = true;
        double heuristic_to_remove_small_parts_at_right_max_ratio = 0.01;
    };

    explicit SimpleMergeSelector(const Settings & settings_) : settings(settings_) {}

    PartsRange select(
        const PartsRanges & parts_ranges,
        const size_t max_total_size_to_merge) override;

private:
    const Settings settings;
};

}
