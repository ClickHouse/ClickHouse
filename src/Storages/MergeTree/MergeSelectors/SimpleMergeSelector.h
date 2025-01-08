#pragma once

#include <Storages/MergeTree/MergeSelectors/MergeSelector.h>


/**
We have a set of data parts that is dynamically changing - new data parts are added and there is background merging process.
Background merging process periodically selects continuous range of data parts to merge.

It tries to optimize the following metrics:
1. Write amplification: total amount of data written on disk (source data + merges) relative to the amount of source data.
It can be also considered as the total amount of work for merges.
2. The number of data parts in the set at the random moment of time (average + quantiles).

Also taking the following considerations:
1. Older data parts should be merged less frequently than newer data parts.
2. Larger data parts should be merged less frequently than smaller data parts.
3. If no new parts arrive, we should continue to merge existing data parts to eventually optimize the table.
4. Never allow too many parts, because it will slow down SELECT queries significantly.
5. Multiple background merges can run concurrently but not too many.

It is not possible to optimize both metrics, because they contradict to each other.
To lower the number of parts we can merge eagerly but write amplification will increase.
Then we need some balance between optimization of these two metrics.

But some optimizations may improve both metrics.

For example, we can look at the "merge tree" - the tree of data parts that were merged.
If the tree is perfectly balanced then its depth is proportional to the log(data size),
the total amount of work is proportional to data_size * log(data_size)
and the write amplification is proportional to log(data_size).
If it's not balanced (e.g. every new data part is always merged with existing data parts),
its depth is proportional to the data size, total amount of work is proportional to data_size^2.

We can also control the "base of the logarithm" - you can look it as the number of data parts
that are merged at once (the tree "arity"). But as the data parts are of different size, we should generalize it:
calculate the ratio between total size of merged parts to the size of the largest part participated in merge.
For example, if we merge 4 parts of size 5, 3, 1, 1 - then "base" will be 2 (10 / 5).

Base of the logarithm (simply called `base` in `SimpleMergeSelector`) is the main knob to control the write amplification.
The more it is, the less is write amplification but we will have more data parts on average.

To fit all the considerations, we also adjust `base` depending on total parts count,
parts size and parts age, with linear interpolation (then `base` is not a constant but a function of multiple variables,
looking like a section of hyperplane).


Then we apply the algorithm to select the optimal range of data parts to merge.
There is a proof that this algorithm is optimal if we look in the future only by single step.

The best range of data parts is selected.

We also apply some tunes:
- there is a fixed const of merging small parts (that is added to the size of data part before all estimations);
- there are some heuristics to "stick" ranges to large data parts.

It's still unclear if this algorithm is good or optimal at all. It's unclear if this algorithm is using the optimal coefficients.

To test and optimize SimpleMergeSelector, we apply the following methods:
- insert/merge simulator: a model simulating parts insertion and merging;
merge selecting algorithm is applied and the relevant metrics are calculated to allow to tune the algorithm;
- insert/merge simulator on real `system.part_log` from production - it gives realistic information about inserted data parts:
their sizes, at what time intervals they are inserted;

There is a research thesis dedicated to optimization of merge algorithm:
https://presentations.clickhouse.com/hse_2019/merge_algorithm.pptx

This work made attempt to variate the coefficients in SimpleMergeSelector and to solve the optimization task:
maybe some change in coefficients will give a clear win on all metrics. Surprisingly enough, it has found
that our selection of coefficients is near optimal. It has found slightly more optimal coefficients,
but I decided not to use them, because the representativeness of the test data is in question.

This work did not make any attempt to propose any other algorithm.
This work did not make any attempt to analyze the task with analytical methods.
That's why I still believe that there are many opportunities to optimize the merge selection algorithm.

Please do not mix the task with a similar task in other LSM-based systems (like RocksDB).
Their problem statement is subtly different. Our set of data parts is consisted of data parts
that are completely independent in stored data. Ranges of primary keys in data parts can intersect.
When doing SELECT we read from all data parts. INSERTed data parts comes with unknown size...
*/

namespace DB
{

class SimpleMergeSelector final : public IMergeSelector
{
public:
    struct Settings
    {
        /// Zero means unlimited. Can be overridden by the same merge tree setting.
        size_t max_parts_to_merge_at_once = 100;

        /// Some sort of a maximum number of parts in partition. Can be overridden by the same merge tree setting.
        size_t parts_to_throw_insert = 3000;

        /** This mode allows selector algorithm not to perform precise comparisons with base (read the comment below).
          * Instead, we do it in an epsilon neighborhood, where epsilon is controlled by the number of parts in
          * the current partition and is a normally distributed random variable.
          */
        bool use_blurry_base = false;
        size_t blurry_base_scale_factor = 42;

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


        size_t window_size = 1000;
        bool enable_stochastic_sliding = false;

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

        /** If it's not 0, all part ranges that have min_age larger than min_age_to_force_merge
          * will be considered for merging
          */
        size_t min_age_to_force_merge = 0;

        /** Heuristic:
          * From right side of range, remove all parts, that size is less than specified ratio of sum_size.
          */
        bool enable_heuristic_to_remove_small_parts_at_right = true;
        double heuristic_to_remove_small_parts_at_right_max_ratio = 0.01;
    };

    explicit SimpleMergeSelector(const Settings & settings_) : settings(settings_) {}

    PartsRange select(
        const PartsRanges & parts_ranges,
        size_t max_total_size_to_merge) override;

private:
    const Settings settings;
};

}
