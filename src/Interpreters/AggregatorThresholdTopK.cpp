#include <algorithm>
#include <cstring>
#include <limits>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/AggregationUtils.h>
#include <Interpreters/Aggregator.h>
#include <base/PackedStringRef.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

/** The top-K threshold merge: an implementation of Fagin's Threshold Algorithm (TA) over the
  * per-thread partial aggregation hash tables, for queries of the form
  *
  *     SELECT keys..., agg(...) FROM t GROUP BY keys ORDER BY agg(...) [DESC] LIMIT n
  *
  * where `agg` declares a `MergedValueBound` (`count`, `uniqExact`, `min`, `max`).
  *
  * After the parallel aggregation each thread holds a two-level hash table with partial states.
  * A bucket of the two-level split holds the same groups in every table, so the ordinary merge
  * (`mergeBucketImpl`) merges the bucket of every table into the first one - touching every
  * group - and only then the conversion materializes it. But the query needs just the n best
  * groups of the bucket (a group outside its own bucket's best n has at least n groups ahead of
  * it globally, so it cannot be in the global top n; the same argument as `Params::bucket_top_k`).
  *
  * TA finds those n groups while merging only the groups that can possibly rank among them:
  *
  * 1. For every table, the values of the partial states are peeked (`insertResultInto` is
  *    guaranteed to keep the state usable) and the bucket's cells are arranged into a heap by
  *    that value - this gives Fagin's "sorted access" without sorting whole lists.
  * 2. Cells are popped in the order of their partial values across all tables. The first time a
  *    group appears, it is looked up in every other table ("random access"), its states are
  *    merged, and its exact merged value ranks it in a bounded heap of the n best candidates.
  *    Consumed cells are nulled, so later pops of the same group are skipped.
  * 3. The threshold: the not-yet-popped partial values of any unseen group are bounded by the
  *    current heap tops, so the merged value of any unseen group is bounded by their combination
  *    - the sum for a `Subadditive` bound with descending order, the best top otherwise. As soon
  *    as that bound cannot strictly beat the worst kept candidate, no unseen group can enter the
  *    result, and the merge stops without touching the remaining groups.
  *
  * The merge and the materialization become sublinear in the number of groups. The extremum
  * bounds of `min`/`max` converge right after the top k candidates are found for any number of
  * tables; the summing bound of `count`/`uniqExact` serves the single-table case (a pure
  * selection - see the commit gate below for why it is not worth committing to across several
  * tables). A pop budget with a bucket-shared verdict backstops the walks that fail to converge.
  */

namespace ProfileEvents
{
    extern const Event AggregationThresholdTopKMerges;
    extern const Event AggregationThresholdTopKMergedGroups;
    extern const Event AggregationThresholdTopKPrunedCells;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// How many cells the bucket must hold (across all tables) per requested group for the threshold
/// machinery to be preferred over the ordinary merge: below that everything is a candidate and
/// the setup cannot pay off.
constexpr size_t min_cells_per_k = 2;

/// How often the merge loop checks for cancellation.
constexpr size_t cancellation_check_period = 256;

UInt64 saturatingAdd(UInt64 a, UInt64 b)
{
    UInt64 sum = 0;
    if (__builtin_add_overflow(a, b, &sum))
        return std::numeric_limits<UInt64>::max();
    return sum;
}

}

std::optional<Aggregator::AggregatedChunk> Aggregator::tryMergeAndConvertOneBucketToChunkThresholdTopK(
    ManyAggregatedDataVariants & variants,
    Arena * arena,
    Int32 bucket,
    std::atomic<bool> & is_cancelled,
    std::atomic<int> * threshold_top_k_verdict) const
{
    auto & merged_data = *variants[0];
    auto method = merged_data.type;

    if (false) {} // NOLINT
#define M(NAME) \
    else if (method == AggregatedDataVariants::Type::NAME) \
        return mergeAndConvertOneBucketToChunkThresholdTopKImpl<decltype(merged_data.NAME)::element_type>( \
            variants, arena, bucket, is_cancelled, threshold_top_k_verdict);

    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M

    return std::nullopt;
}

template <typename Method>
requires SetAggregationMethod<Method>
std::optional<Aggregator::AggregatedChunk> Aggregator::mergeAndConvertOneBucketToChunkThresholdTopKImpl(
    ManyAggregatedDataVariants &, Arena *, Int32, std::atomic<bool> &, std::atomic<int> *) const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "The top-K threshold merge does not support set methods");
}

template <typename Method>
requires MapAggregationMethod<Method>
std::optional<Aggregator::AggregatedChunk> Aggregator::mergeAndConvertOneBucketToChunkThresholdTopKImpl(
    ManyAggregatedDataVariants & variants,
    Arena * arena,
    Int32 bucket,
    std::atomic<bool> & is_cancelled,
    std::atomic<int> * threshold_top_k_verdict) const
{
    /// The plan never requests the threshold merge for nullable or low-cardinality keys (their
    /// tables carry a special null-key cell the walk below does not visit), but the dispatch
    /// above instantiates this template for every two-level method.
    if constexpr (Method::low_cardinality_optimization || Method::one_key_nullable_optimization)
        return std::nullopt;
    else
    {
        auto & merged_data = *variants[0];
        const auto & top_k = *params.threshold_top_k;
        const size_t k = top_k.k;
        const bool ascending = top_k.ascending;
        const size_t order_by_index = top_k.aggregate_index;
        const bool additive_descending = top_k.bound == MergedValueBound::Subadditive && !ascending;
        /// The `Subadditive` bound sums partial values, so they must be numbers; the plan
        /// guarantees it by requiring a `UInt64` result for that bound. `Maximum`/`Minimum`
        /// bounds only compare values, so any comparable type works through the column.
        const bool values_are_uint64 = top_k.bound == MergedValueBound::Subadditive;
        chassert(order_by_index < params.aggregates_size);
        chassert(!is_simple_count || values_are_uint64);

        using Table = std::decay_t<decltype(getDataVariant<Method>(merged_data).data.impls[bucket])>;
        using TableKey = std::decay_t<decltype(std::declval<const typename Table::cell_type &>().getKey())>;

        std::vector<Table *> tables;
        tables.reserve(variants.size());
        size_t total_cells = 0;
        for (const auto & variant : variants)
        {
            Table & table = getDataVariant<Method>(*variant).data.impls[bucket];
            if (table.empty())
                continue;
            tables.push_back(&table);
            total_cells += table.size();
        }

        /// When (almost) every group is a candidate anyway, the ordinary merge is cheaper.
        if (total_cells <= k * min_cells_per_k)
            return std::nullopt;

        /// The summing bound serves only a dominant table (e.g. after the adaptive aggregator
        /// drained the frozen tables into one, with at most stray leftovers elsewhere), where it
        /// is nearly a pure selection that stops right after the top k groups. With the groups
        /// truly split across the tables it is not worth committing to: with near-uniform values
        /// the sum of the heads never lets the walk stop, and even when it converges, the groups
        /// it prunes are exactly the ones whose states are the cheapest to merge (they lost the
        /// ranking), so the measured outcome is parity at best. The extremum bounds converge
        /// right after the candidate heap fills for any number of tables.
        if (additive_descending)
        {
            size_t largest_table_cells = 0;
            for (const Table * table : tables)
                largest_table_cells = std::max(largest_table_cells, table->size());
            if (largest_table_cells * 8 < total_cells * 7)
                return std::nullopt;
        }

        /// An earlier bucket exhausted the walk budget: this value distribution does not converge.
        if (threshold_top_k_verdict && threshold_top_k_verdict->load(std::memory_order_relaxed) == 0)
            return std::nullopt;

        const IAggregateFunction * order_by_function = aggregate_functions[order_by_index];
        const size_t order_by_offset = offsets_of_aggregate_states[order_by_index];

        /// See the same handling in `convertOneBucketToChunk`: the adaptive merge's per-bucket
        /// arena must be handed to the output columns together with the ordinary pools.
        Arenas * pools_for_output = &merged_data.aggregates_pools;
        Arenas pools_with_bucket_arena;
        if (!merged_data.adaptive_merge_bucket_arenas.empty())
        {
            pools_with_bucket_arena = merged_data.aggregates_pools;
            pools_with_bucket_arena.push_back(merged_data.adaptive_merge_bucket_arenas[bucket]);
            pools_for_output = &pools_with_bucket_arena;
        }

        /// Fagin's sorted access: every table's cells with the partial value of the ordering
        /// aggregate, arranged as a heap ("sorted lists" materialized lazily).
        struct Entry
        {
            TableKey key;
            AggregateDataPtr * slot;    /// the cell's mapped slot; consumed cells are zeroed
            UInt32 value_index;         /// into the list's values (entries are permuted by the heap)
        };

        struct List
        {
            Table * table = nullptr;
            std::vector<Entry> entries;
            /// The cells' states in the entries' order, for the batch peek of their values.
            PaddedPODArray<AggregateDataPtr> places;
            /// The peeked partial values: a column for the generic path, a plain array when the
            /// values are known to be UInt64 (the inline count has no state to peek from).
            MutableColumnPtr values_column;
            PaddedPODArray<UInt64> inline_counts;
            const UInt64 * values_uint = nullptr;
            size_t heap_size = 0;
        };

        std::vector<List> lists(tables.size());

        /// One walk per table collects the cells; the values are peeked afterwards in batches.
        for (size_t i = 0; i < tables.size(); ++i)
        {
            List & list = lists[i];
            list.table = tables[i];
            const size_t cells = tables[i]->size();
            list.entries.reserve(cells);
            if (is_simple_count)
            {
                list.inline_counts.reserve(cells);
                tables[i]->forEachValue(
                    [&](const auto & key, auto & mapped)
                    {
                        list.inline_counts.push_back(getInlineCountState(mapped));
                        list.entries.push_back(Entry{key, &mapped, static_cast<UInt32>(list.entries.size())});
                    });
                list.values_uint = list.inline_counts.data();
            }
            else
            {
                list.places.reserve(cells);
                tables[i]->forEachValue(
                    [&](const auto & key, auto & mapped)
                    {
                        list.places.push_back(mapped);
                        list.entries.push_back(Entry{key, &mapped, static_cast<UInt32>(list.entries.size())});
                    });
            }
            list.heap_size = list.entries.size();
        }

        ProfileEvents::increment(ProfileEvents::AggregationThresholdTopKMerges);

        /// Peek the partial values of every cell (the inline count already collected them).
        if (!is_simple_count)
        {
            for (List & list : lists)
            {
                list.values_column = order_by_function->getResultType()->createColumn();
                list.values_column->reserve(list.places.size());
                order_by_function->insertResultIntoBatchWithoutDestroying(
                    0, list.places.size(), list.places.data(), order_by_offset, *list.values_column, arena);
                if (values_are_uint64)
                    list.values_uint = assert_cast<const ColumnUInt64 &>(*list.values_column).getData().data();
            }
        }

        /// `better(a, b)`: a ranks strictly before b in the requested order. The generic
        /// comparison ranks NaNs consistently with the min/max merge itself, which ignores a NaN
        /// unless there is nothing else; the plan excludes floating-point ordering values anyway.
        const auto better_uint = [ascending](UInt64 a, UInt64 b) { return ascending ? a < b : a > b; };
        const int nan_direction_hint = top_k.bound == MergedValueBound::Maximum ? -1 : 1;
        const auto better_column = [ascending, nan_direction_hint](const IColumn & a_col, size_t a_row, const IColumn & b_col, size_t b_row)
        {
            const int cmp = a_col.compareAt(a_row, b_row, b_col, nan_direction_hint);
            return ascending ? cmp < 0 : cmp > 0;
        };

        const auto entry_better = [&](const List & list, const Entry & a, const Entry & b)
        {
            if (values_are_uint64)
                return better_uint(list.values_uint[a.value_index], list.values_uint[b.value_index]);
            return better_column(*list.values_column, a.value_index, *list.values_column, b.value_index);
        };

        for (List & list : lists)
        {
            std::make_heap(
                list.entries.begin(), list.entries.end(),
                [&](const Entry & a, const Entry & b) { return entry_better(list, b, a); });
        }

        /// Is the head of list a strictly better than the head of list b?
        const auto head_better = [&](const List & a, const List & b)
        {
            if (values_are_uint64)
                return better_uint(a.values_uint[a.entries.front().value_index], b.values_uint[b.entries.front().value_index]);
            return better_column(
                *a.values_column, a.entries.front().value_index, *b.values_column, b.entries.front().value_index);
        };

        /// The bounded heap of the bucket's best candidates, the worst kept one at the front.
        /// For the UInt64 path `value` is the exact merged value; otherwise it is a row in
        /// `exact_values_column` (append-only, rows of rejected candidates stay as garbage).
        struct Candidate
        {
            TableKey key;
            AggregateDataPtr place;    /// nullptr for the inline-count path
            UInt64 value;
        };

        std::vector<Candidate> candidates;
        candidates.reserve(k);
        MutableColumnPtr exact_values_column;
        MutableColumnPtr exact_value_scratch;
        if (values_are_uint64)
        {
            if (!is_simple_count)
                exact_value_scratch = ColumnUInt64::create();
        }
        else
            exact_values_column = order_by_function->getResultType()->createColumn();

        const auto candidate_worse = [&](const Candidate & a, const Candidate & b)
        {
            if (values_are_uint64)
                return better_uint(a.value, b.value);
            return better_column(*exact_values_column, a.value, *exact_values_column, b.value);
        };

        const auto destroy_place = [&](AggregateDataPtr place)
        {
            if (all_aggregates_has_trivial_destructor || is_simple_count)
                return;
            for (size_t i = 0; i < params.aggregates_size; ++i)
                if (!aggregate_functions[i]->hasTrivialDestructor())
                    aggregate_functions[i]->destroy(place + offsets_of_aggregate_states[i]);
        };

        const auto destroy_candidates = [&]
        {
            for (const Candidate & candidate : candidates)
                if (candidate.place)
                    destroy_place(candidate.place);
            candidates.clear();
        };

        /// The packed-string cells hand out an unpacked view, but probing a table needs the raw
        /// packed key; rebuild it with the method's content hash. The small encoding's `build`
        /// reads whole words starting at the key, and a view of another cell does not guarantee
        /// the required padding, so small keys go through a padded copy.
        const auto find_in = [](Table & table, const TableKey & key)
        {
            if constexpr (std::is_same_v<typename Method::Key, PackedStringRef>)
            {
                using PackedHash = typename Method::State::Hash;
                const size_t size = key.size();
                if (size <= PackedStringRef::MAX_SMALL_LEN)
                {
                    char padded[PackedStringRef::MAX_SMALL_LEN + 8] = {};
                    memcpy(padded, key.data(), size);
                    return table.find(PackedStringRef::build(padded, size, PackedHash{}));
                }
                /// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage): the size is passed alongside
                return table.find(PackedStringRef::build(key.data(), size, PackedHash{}));
            }
            else
                return table.find(key);
        };

        size_t merged_groups = 0;
        /// Every cell is consumed exactly once: either popped while still live, or found by a
        /// random access (which nulls it, so a later pop of the same cell is skipped).
        size_t consumed_cells = 0;
        size_t popped_cells = 0;

        /// Fagin's random access for one cell: collect and merge the group's states from every
        /// other table, zeroing the consumed cells so later encounters of the group are skipped,
        /// and rank the exact merged value in the bounded candidate heap.
        const auto process_cell = [&](const Entry & entry, Table * entry_table)
        {
            /// Consumed by an earlier random access for the same group.
            if (is_simple_count ? (getInlineCountState(*entry.slot) == 0) : (*entry.slot == nullptr))
                return;

            ++consumed_cells;
            ++merged_groups;
            Candidate candidate{entry.key, nullptr, 0};

            if (is_simple_count)
            {
                UInt64 count = getInlineCountState(*entry.slot);
                getInlineCountState(*entry.slot) = 0;
                for (Table * table : tables)
                {
                    if (table == entry_table)
                        continue;
                    auto it = find_in(*table, entry.key);
                    if (!it)
                        continue;
                    ++consumed_cells;
                    count += getInlineCountState(it->getMapped());
                    getInlineCountState(it->getMapped()) = 0;
                }
                candidate.value = count;
            }
            else
            {
                AggregateDataPtr place = *entry.slot;
                *entry.slot = nullptr;
                try
                {
                    for (Table * table : tables)
                    {
                        if (table == entry_table)
                            continue;
                        auto it = find_in(*table, entry.key);
                        if (!it)
                            continue;
                        AggregateDataPtr & other_slot = it->getMapped();
                        if (!other_slot)
                            continue;
                        ++consumed_cells;
                        /// Detach the source state from the table before the destructive merge
                        /// (the same pattern as `mergeDataImpl`): if a later aggregate's merge
                        /// throws, the table cell must not be destroyed again by the unwinding -
                        /// the already-consumed substates would be destroyed a second time.
                        AggregateDataPtr other = other_slot;
                        other_slot = nullptr;
                        /// Always the interpreted merge, even when the accumulation was
                        /// JIT-compiled: the compiled and the interpreted code share the
                        /// state layout by contract (spilling and distributed aggregation
                        /// rely on the same interoperability), and only the few candidate
                        /// groups pay for it. The per-pair batch entry point handles the
                        /// functions whose merge may use the thread pool (e.g. `uniqExact`)
                        /// and destroys the source state.
                        for (size_t f = 0; f < params.aggregates_size; ++f)
                            aggregate_functions[f]->mergeAndDestroyBatch(
                                &place, &other, 1, offsets_of_aggregate_states[f], *thread_pool, is_cancelled, arena);
                    }

                    /// The exact merged value of the group.
                    if (values_are_uint64)
                    {
                        order_by_function->insertResultInto(place + order_by_offset, *exact_value_scratch, arena);
                        candidate.value = assert_cast<const ColumnUInt64 &>(*exact_value_scratch).getData().back();
                        exact_value_scratch->popBack(1);
                    }
                    else
                    {
                        order_by_function->insertResultInto(place + order_by_offset, *exact_values_column, arena);
                        candidate.value = exact_values_column->size() - 1;
                    }
                }
                catch (...)
                {
                    destroy_place(place);
                    throw;
                }
                candidate.place = place;
            }

            if (candidates.size() < k)
            {
                candidates.push_back(candidate);
                std::push_heap(candidates.begin(), candidates.end(), candidate_worse);
            }
            else if (candidate_worse(candidates.front(), candidate))
            {
                std::pop_heap(candidates.begin(), candidates.end(), candidate_worse);
                if (candidates.back().place)
                    destroy_place(candidates.back().place);
                candidates.back() = candidate;
                std::push_heap(candidates.begin(), candidates.end(), candidate_worse);
            }
            else
            {
                /// The group's exact value is final; once rejected it can never enter.
                if (candidate.place)
                    destroy_place(candidate.place);
            }
        };

        /// A safety net for the walks that fail to converge - e.g. `ORDER BY min(x) DESC`, where
        /// a group's per-table partial minima can all sit far above its merged minimum, keeping
        /// the threshold open (the favorable directions stop within about k * m pops). Once half
        /// of the cells are popped with the threshold still open, this bucket finishes with a
        /// plain linear sweep - same processing, no heap traffic - and the verdict tells the
        /// remaining buckets (which see the same hash-partitioned distribution) to take the
        /// ordinary merge from the start.
        const size_t pop_budget = total_cells / 2;
        bool budget_exhausted = false;

        try
        {
            while (!lists.empty())
            {
                /// Pick the list whose head ranks first: that head is the next sorted access,
                /// and (for the non-summing bounds) also the threshold itself.
                size_t best_list = 0;
                for (size_t i = 1; i < lists.size(); ++i)
                    if (head_better(lists[i], lists[best_list]))
                        best_list = i;

                if (candidates.size() == k)
                {
                    /// The threshold check: can any unseen group still strictly beat the worst
                    /// kept candidate? An unseen group's partial values are bounded by the current
                    /// heads of the lists it may still hide in, so for the descending `Subadditive`
                    /// bound its merged value is at most the sum of the heads, and otherwise (the
                    /// exact extremum bounds, and any bound with the ascending order, where the
                    /// merged value is no better than some partial value) - the best head.
                    const Candidate & worst = candidates.front();
                    bool can_beat = false;
                    if (additive_descending)
                    {
                        UInt64 threshold = 0;
                        for (const List & list : lists)
                            threshold = saturatingAdd(threshold, list.values_uint[list.entries.front().value_index]);
                        can_beat = threshold > worst.value;
                    }
                    else if (values_are_uint64)
                        can_beat = better_uint(lists[best_list].values_uint[lists[best_list].entries.front().value_index], worst.value);
                    else
                        can_beat = better_column(
                            *lists[best_list].values_column,
                            lists[best_list].entries.front().value_index,
                            *exact_values_column,
                            worst.value);

                    if (!can_beat)
                        break;
                }

                /// Pop the best head.
                List & list = lists[best_list];
                std::pop_heap(
                    list.entries.begin(), list.entries.begin() + list.heap_size,
                    [&](const Entry & a, const Entry & b) { return entry_better(list, b, a); });
                --list.heap_size;
                const Entry entry = list.entries[list.heap_size];
                Table * entry_table = list.table;
                if (list.heap_size == 0)
                {
                    if (best_list != lists.size() - 1)
                        std::swap(lists[best_list], lists.back());
                    lists.pop_back();
                }

                if ((++popped_cells % cancellation_check_period) == 0 && is_cancelled.load(std::memory_order_seq_cst))
                {
                    destroy_candidates();
                    return AggregatedChunk{};
                }

                process_cell(entry, entry_table);

                if (popped_cells > pop_budget)
                {
                    if (threshold_top_k_verdict)
                        threshold_top_k_verdict->store(0, std::memory_order_relaxed);
                    budget_exhausted = true;
                    break;
                }
            }

            /// The budget ran out: no more sorted access, just process what remains (the first
            /// `heap_size` entries of every surviving list are exactly the unpopped ones).
            if (budget_exhausted)
            {
                for (List & list : lists)
                {
                    for (size_t j = 0; j < list.heap_size; ++j)
                    {
                        if ((++popped_cells % cancellation_check_period) == 0 && is_cancelled.load(std::memory_order_seq_cst))
                        {
                            destroy_candidates();
                            return AggregatedChunk{};
                        }
                        process_cell(list.entries[j], list.table);
                    }
                }
            }

            ProfileEvents::increment(ProfileEvents::AggregationThresholdTopKMergedGroups, merged_groups);
            ProfileEvents::increment(ProfileEvents::AggregationThresholdTopKPrunedCells, total_cells - consumed_cells);

            /// Materialize the candidates, exactly like the ordinary conversion would.
            auto & method_object = getDataVariant<Method>(merged_data);
            const size_t keep = candidates.size();
            auto out_cols = prepareOutputBlockColumns(
                params, aggregate_functions, key_types, aggregate_state_types, *pools_for_output, /*final=*/true, keep);
            auto shuffled_key_sizes = method_object.shuffleKeyColumns(out_cols.raw_key_columns, key_sizes);
            const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes : key_sizes;
            IColumn::SerializationSettings serialization_settings{
                .serialize_string_with_zero_byte = params.serialize_string_with_zero_byte};

            PaddedPODArray<AggregateDataPtr> places;
            places.reserve(keep);
            for (const Candidate & candidate : candidates)
            {
                method_object.insertKeyIntoColumns(candidate.key, out_cols.raw_key_columns, key_sizes_ref, &serialization_settings);
                if (is_simple_count)
                    assert_cast<ColumnUInt64 &>(*out_cols.final_aggregate_columns[0]).getData().push_back(candidate.value);
                else
                    places.push_back(candidate.place);
            }
            /// The states now belong to `insertResultsIntoColumns` (it destroys them after
            /// inserting the results); nothing to roll back from here on.
            candidates.clear();

            Chunk chunk;
            if (is_simple_count)
                chunk = finalizeChunk(params, std::move(out_cols), /*final=*/true);
            else
                chunk = insertResultsIntoColumns(places, std::move(out_cols), arena, /*has_null_key_data=*/false, /*use_compiled_functions=*/false);

            /// The unseen groups were never merged: destroy their states right in the tables.
            if (!all_aggregates_has_trivial_destructor && !is_simple_count)
            {
                for (Table * table : tables)
                {
                    table->forEachValue(
                        [&](const auto &, auto & mapped)
                        {
                            if (!mapped)
                                return;
                            destroy_place(mapped);
                            mapped = nullptr;
                        });
                }
            }

            for (Table * table : tables)
                table->clearAndShrink();

            return AggregatedChunk{std::move(chunk), bucket};
        }
        catch (...)
        {
            /// The candidates' merged states live outside the hash tables, so the ordinary
            /// destruction of the variants would not find them.
            destroy_candidates();
            throw;
        }
    }
}

}
