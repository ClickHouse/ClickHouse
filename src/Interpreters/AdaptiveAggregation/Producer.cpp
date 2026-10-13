/// The producer kernels of the adaptive aggregation: the frozen consume path and the staging of missed rows as
/// partition records. They are member templates of `Aggregator` (defined here rather than in `Aggregator.cpp`,
/// following `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <limits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/Arena.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/memcpySmall.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/RowDataStore.h>

namespace ProfileEvents
{
    extern const Event AggregationOptimizedEqualRangesOfKeys;
    extern const Event AdaptiveAggregationProbeBypasses;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

namespace
{
    /// Copies a string-like key of the block into a staged record. A state that exposes its padded column buffers is
    /// read directly, with the overflow-tolerant small copy those buffers allow: it reads and writes up to 15 bytes past
    /// the end, and the records are appended in increasing byte order with every chunk ending in unused padding, so the
    /// scribble lands in space the next append overwrites or that no record uses. Another state's key holder may own the
    /// bytes in an exact-size allocation or roll its scratch-arena allocation back on discard, so its bytes are copied
    /// exactly, while the holder is alive.
    template <typename State>
    void ALWAYS_INLINE copyStagedKeyBytes(State & state, size_t row, size_t size, DB::Arena & scratch, char * staged)
    {
        if (!size)
            return;
        if constexpr (requires { state.getKeyBytes(row); })
        {
            const char * data = state.getKeyBytes(row).data();
            if (size <= 64)
                memcpySmallAllowReadWriteOverflow15(staged, data, size);
            else
                memcpy(staged, data, size);
        }
        else
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            const auto key = static_cast<std::string_view>(keyHolderGetKey(key_holder));
            chassert(key.size() == size);
            memcpy(staged, key.data(), size);
            keyHolderDiscardKey(key_holder);
        }
    }

    /// Records the key of a staged miss for the append: the size of a string-like key, whose bytes the append copies
    /// out of the block, or the value of a fixed-width key, which the kernel has at hand, while reading it back from
    /// the key columns would pack it a second time.
    template <typename RecordKey, typename Key>
    void ALWAYS_INLINE recordStagedKey(DB::AdaptiveAggregationProducer & adaptive, const Key & key)
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
        {
            adaptive.miss_key_sizes.push_back(static_cast<std::string_view>(key).size());
        }
        else
        {
            const RecordKey staged = key;
            const char * bytes = reinterpret_cast<const char *>(&staged);
            adaptive.miss_keys.insert(bytes, bytes + sizeof(staged));
        }
    }

}

void Aggregator::executeFrozen(
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    AggregatedDataVariants & result,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    /// The rows of the block are the base of the thaw's staged share (see `adaptiveStagingRepeats`).
    std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase).rows += row_end - row_begin;

#define M(NAME) \
    else if (result.type == AggregatedDataVariants::Type::NAME) \
        executeFrozenImpl( \
            *result.NAME, \
            std::type_identity<std::decay_t<decltype(*result.NAME##_two_level)>>{}, \
            result.aggregates_pool, \
            columns, \
            row_begin, \
            row_end, \
            key_columns, \
            aggregate_instructions, \
            adaptive, \
            all_keys_are_const);

    if (false) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
#undef M
    else
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive frozen path.");
}

/// The set counterpart of the frozen kernel. A `GROUP BY` without aggregate functions has no places to
/// record and no states to advance, so a hit costs nothing beyond the probe and a miss stages the key
/// alone - which is all a set has to carry.
template <typename LocalMethod, typename SharedMethod>
requires SetAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena *,
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction *,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    using RecordKey = AdaptiveRecordKey<SharedMethod>;
    static_assert(SharedMethod::Data::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);

    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const bool bypass_local_probe = frozen.bypass_local_probe;

    auto stage_miss = [&]([[maybe_unused]] const auto & key, UInt64 hash, size_t row)
    {
        adaptive.miss_source_rows.push_back(static_cast<UInt32>(row));
        adaptive.miss_hashes.push_back(hash);
        recordStagedKey<RecordKey>(adaptive, key);
    };

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        /// The whole range carries one key and a set stores a key once, so a single record stands
        /// for it however many rows it spans.
        const auto found = local_method.data.find(key, hash);
        if (!found)
        {
            stage_miss(key, hash, row_begin);
            appendDelayedRecords<RecordKey>(
                columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false, /*key_row_override=*/0);
        }
        if (frozen.recordLocalProbeResults(found ? row_end - row_begin : 0, row_end - row_begin))
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        keyHolderDiscardKey(key_holder);
        return;
    }

    size_t hits = 0;
    for (size_t i = row_begin; i < row_end; ++i)
    {
        auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        if (!bypass_local_probe && local_method.data.find(key, hash))
            ++hits;
        else
            stage_miss(key, hash, i);

        keyHolderDiscardKey(key_holder);
    }

    if (frozen.recordLocalProbeResults(hits, row_end - row_begin))
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);

    appendDelayedRecords<RecordKey>(
        columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
}

template <typename LocalMethod, typename SharedMethod>
requires MapAggregationMethod<LocalMethod>
void NO_INLINE Aggregator::executeFrozenImpl(
    LocalMethod & local_method,
    std::type_identity<SharedMethod>,
    Arena * aggregates_pool,
    const Columns & columns,
    size_t row_begin,
    size_t row_end,
    ColumnRawPtrs & key_columns,
    AggregateFunctionInstruction * aggregate_instructions,
    AdaptiveAggregationProducer & adaptive,
    bool all_keys_are_const) const
{
    using RecordKey = AdaptiveRecordKey<SharedMethod>;
    static_assert(SharedMethod::Data::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);

    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);
    /// Routing needs only the two-level twin's TYPE: the hash is the local table's canonical
    /// hash (identical to the twin's by construction - the pairing in `executeFrozen` binds a
    /// method to its own two-level form, which keeps the hash function), and the partitioning
    /// of the hash is static.

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    const auto record_local_probes = [&](size_t hits, size_t rows)
    {
        if (frozen.recordLocalProbeResults(hits, rows))
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
    };
    const bool bypass_local_probe = frozen.bypass_local_probe;

    if (all_keys_are_const)
    {
        auto && key_holder = local_find_state.getKeyHolder(0, scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);
        const UInt64 hash = local_method.data.hash(key);

        bool found = false;
        AggregateDataPtr found_place = nullptr;
        if (auto it = local_method.data.find(key, hash))
        {
            found = true;
            if (is_simple_count)
                getInlineCountState(it->getMapped()) += row_end - row_begin;
            else
                found_place = it->getMapped();
        }

        if (found)
        {
            if (!is_simple_count && params.aggregates_size)
            {
                /// Apply the whole range to the single place, mirroring the ordinary
                /// all-keys-are-const handling.
                for (size_t i = 0; i < aggregate_functions.size(); ++i)
                {
                    AggregateFunctionInstruction * inst = aggregate_instructions + i;
                    ProfileEvents::increment(ProfileEvents::AggregationOptimizedEqualRangesOfKeys);
                    addBatchSinglePlace(row_begin, row_end, inst, found_place + inst->state_offset, aggregates_pool);
                }
            }
        }
        else
        {
            if (is_simple_count)
            {
                adaptive.miss_hashes.push_back(hash);
                adaptive.miss_multiplicities.push_back(static_cast<UInt32>(row_end - row_begin));
                recordStagedKey<RecordKey>(adaptive, key);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
                    adaptive.miss_hashes.push_back(hash);
                    recordStagedKey<RecordKey>(adaptive, key);
                }
            }
            appendDelayedRecords<RecordKey>(
                columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/is_simple_count, /*key_row_override=*/0);
        }
        record_local_probes(found ? row_end - row_begin : 0, row_end - row_begin);
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        constexpr bool can_combine_partition_counts = adaptive_key_stages_bytes<RecordKey>
            && requires { local_find_state.getKeyBytes(0); };
        [[maybe_unused]] const bool stable_key_views = local_find_state.keyViewsAreBlockStable();
        struct PartitionCountEntry
        {
            RecordKey key{};
            size_t record_index = std::numeric_limits<size_t>::max();
        };
        std::vector<PartitionCountEntry> last_partition_keys;
        const AdaptivePartitionLayout layout = adaptive.session->layout;
        if constexpr (can_combine_partition_counts)
        {
            chassert(stable_key_views);
            chassert(row_end - row_begin <= std::numeric_limits<UInt32>::max());
            last_partition_keys.resize(layout.numPartitions());
        }

        struct CountMissesResult
        {
            size_t next_row;
            size_t hits;
        };
        const auto collect_count_misses = [&, row_end, bypass_local_probe, layout]
            <bool combine_partition_counts>(size_t begin) ALWAYS_INLINE -> CountMissesResult
        {
            size_t hits = 0;
            /// Only this pass's new descriptors seed consecutive runs. The trial's last missed key may
            /// refer to an earlier descriptor rather than the last one in the array.
            const size_t first_record = can_combine_partition_counts ? adaptive.miss_hashes.size() : 0;
            size_t last_record = 0;
            size_t combining_lookups = 0;
            size_t combined_records = 0;
            bool stop_combining = false;
            RecordKey last_staged_key{};
            for (size_t i = begin; i < row_end; ++i)
            {
                auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
                const auto & key = keyHolderGetKey(key_holder);
                const UInt64 hash = local_method.data.hash(key);

                if (!bypass_local_probe)
                {
                    if (auto it = local_method.data.find(key, hash))
                    {
                        ++hits;
                        ++getInlineCountState(it->getMapped());
                        keyHolderDiscardKey(key_holder);
                        continue;
                    }
                }

                const RecordKey staged_key = key;

                /// Without combining, the previous record is always the last descriptor. Spelling it so keeps
                /// `first_record` and `last_record` out of the loop of the keys that never combine.
                bool run_continues = (can_combine_partition_counts
                    ? adaptive.miss_hashes.size() != first_record : !adaptive.miss_hashes.empty())
                    && (combine_partition_counts ? adaptive.miss_hashes[last_record] : adaptive.miss_hashes.back()) == hash;
                if constexpr (std::is_same_v<RecordKey, std::string_view>)
                    run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
                else
                    run_continues = run_continues && staged_key == last_staged_key;

                if (run_continues)
                {
                    auto & multiplicity = combine_partition_counts
                        ? adaptive.miss_multiplicities[last_record] : adaptive.miss_multiplicities.back();
                    ++multiplicity;
                }
                else
                {
                    size_t record = adaptive.miss_hashes.size();
                    if constexpr (combine_partition_counts)
                    {
                        /// Keep each partition's last missed key for this block. Its bytes remain alive until
                        /// the append copies them into owned records; only the descriptor's count changes here.
                        /// Packed-key equality checks its inline signature before comparing the string bytes.
                        auto & last = last_partition_keys[layout.partitionOf(hash)];
                        if (last.record_index != std::numeric_limits<size_t>::max()
                            && (std::is_same_v<RecordKey, PackedStringRef> || adaptive.miss_hashes[last.record_index] == hash)
                            && staged_key == last.key)
                        {
                            record = last.record_index;
                            ++combined_records;
                        }
                        else
                        {
                            last = {staged_key, record};
                        }
                        ++combining_lookups;
                        if (combining_lookups == adaptive_count_combining_sample_records
                            && combined_records * adaptive_count_combining_min_hit_rate_inverse < combining_lookups)
                        {
                            stop_combining = true;
                        }
                    }

                    if (record == adaptive.miss_hashes.size())
                    {
                        adaptive.miss_hashes.push_back(hash);
                        adaptive.miss_multiplicities.push_back(1);
                        /// Fixed-size keys stage no size: it is a compile-time constant the append
                        /// substitutes, so the hot staging loop skips a dead store per record.
                        recordStagedKey<RecordKey>(adaptive, staged_key);
                        adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
                    }
                    else
                    {
                        ++adaptive.miss_multiplicities[record];
                    }
                    last_record = record;

                    /// A serialized key view points into the reused scratch arena and can only seed
                    /// the run tracking when the views are block-stable; every other key type is
                    /// either a self-contained value or, for a packed reference, points into the
                    /// block's key column, whose bytes remain alive throughout the row loop.
                    if constexpr (std::is_same_v<RecordKey, std::string_view>)
                    {
                        if (stable_key_views)
                            last_staged_key = staged_key;
                    }
                    else
                    {
                        last_staged_key = staged_key;
                    }
                }
                keyHolderDiscardKey(key_holder);
                if constexpr (combine_partition_counts)
                {
                    if (stop_combining)
                        return {i + 1, hits};
                }
            }
            return {row_end, hits};
        };

        size_t hits = 0;
        if constexpr (can_combine_partition_counts)
        {
            const auto trial = collect_count_misses.template operator()<true>(row_begin);
            hits = trial.hits;
            if (trial.next_row != row_end)
            {
                /// Keep the trial's descriptors for the common append. The remainder tracks consecutive
                /// runs among its own new descriptors and performs no partition-cache lookups.
                hits += collect_count_misses.template operator()<false>(trial.next_row).hits;
            }
        }
        else
        {
            hits = collect_count_misses.template operator()<false>(row_begin).hits;
        }
        record_local_probes(hits, row_end - row_begin);
        appendDelayedRecords<RecordKey>(columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/true);
        return;
    }

    /// The probe/staging loop, shared by the with-places and the zero-aggregates shapes: a
    /// keyed GROUP BY without aggregate functions needs no places at all (the baseline
    /// specializes the same way), so it skips the allocation and the per-row stores.
    auto probe_rows = [&]<bool record_places>(AggregateDataPtr * places_data) -> size_t
    {
        size_t hits = 0;
        for (size_t i = row_begin; i < row_end; ++i)
        {
            auto && key_holder = local_find_state.getKeyHolder(i, scratch_pool);
            const auto & key = keyHolderGetKey(key_holder);
            const UInt64 hash = local_method.data.hash(key);

            if (!bypass_local_probe)
            {
                if (auto it = local_method.data.find(key, hash))
                {
                    ++hits;
                    if constexpr (record_places)
                        places_data[i] = it->getMapped();
                    keyHolderDiscardKey(key_holder);
                    continue;
                }
            }

            if constexpr (record_places)
                places_data[i] = nullptr;
            adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
            adaptive.miss_hashes.push_back(hash);

            recordStagedKey<RecordKey>(adaptive, key);
            keyHolderDiscardKey(key_holder);
        }
        return hits;
    };

    /// Without aggregate functions there is nothing to record for a hit; with the probe bypassed there is no hit.
    if (params.aggregates_size == 0 || bypass_local_probe)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        record_local_probes(hits, row_end - row_begin);
        appendDelayedRecords<RecordKey>(columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
        return;
    }

    AllocatorWithMemoryTracking<AggregateDataPtr> allocator;
    const size_t places_size = row_end;
    auto places_deleter = [&allocator, &places_size](auto * ptr)
    {
        if (ptr) [[likely]]
            allocator.deallocate(ptr, places_size);
    };
    std::unique_ptr<AggregateDataPtr[], decltype(places_deleter)> places(allocator.allocate(places_size), places_deleter);

    const size_t hits = probe_rows.template operator()<true>(places.get());
    record_local_probes(hits, row_end - row_begin);
    appendDelayedRecords<RecordKey>(columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);

    /// With no local hits every place is null and the batch pass would only skip rows; the
    /// staged records carry the block's whole contribution.
    if (hits != 0)
        executeAggregateInstructions(
            aggregates_pool,
            row_begin,
            row_end,
            aggregate_instructions,
            places.get(),
            /*key_start=*/row_begin,
            /*has_only_one_value_since_last_reset=*/false,
            /*all_keys_are_const=*/false,
            /*all_places_are_non_null=*/false,
            /*use_compiled_functions=*/false);
}

template <typename RecordKey, typename State>
void NO_INLINE Aggregator::appendDelayedRecords(
    const Columns & columns,
    AdaptiveAggregationProducer & adaptive,
    State & local_find_state,
    Arena & scratch_pool,
    bool counts_only,
    std::optional<UInt32> key_row_override) const
{
    const size_t total = adaptive.miss_hashes.size();
    if (!total)
        return;

    auto & partitions = *adaptive.partitions;
    const AdaptivePartitionLayout layout = adaptive.session->layout;
    const auto * hashes = adaptive.miss_hashes.data();
    const auto * source_rows = adaptive.miss_source_rows.data();
    const auto * staged_key_sizes = adaptive.miss_key_sizes.data();
    const char * staged_keys = adaptive.miss_keys.data();

    /// A constant key is read from the key columns' single row, while the arguments of each record stay its own
    /// source row.
    const auto key_row_of = [source_rows, key_row_override](size_t record) -> size_t
    {
        return key_row_override ? *key_row_override : source_rows[record];
    };
    const auto key_size_of = [&](size_t record) -> size_t
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            return staged_key_sizes[record];
        else
            return sizeof(RecordKey);
    };

    /// The appends go to random partitions, so the loops below prefetch the append position of
    /// the record `adaptive_append_prefetch_distance` ahead.
    const auto prefetch_append = [&partitions, layout, hashes, total](size_t record) ALWAYS_INLINE
    {
        if (record + adaptive_append_prefetch_distance < total)
            partitions.prefetchAppend(layout.partitionOf(hashes[record + adaptive_append_prefetch_distance]));
    };

    const auto write_key = [&](size_t record, size_t key_size, char * to)
    {
        if constexpr (adaptive_key_stages_bytes<RecordKey>)
            copyStagedKeyBytes(local_find_state, key_row_of(record), key_size, scratch_pool, to);
        else
            memcpy(to, staged_keys + record * sizeof(RecordKey), sizeof(RecordKey));
    };

    size_t key_bytes = 0;
    size_t variable_argument_bytes = 0;

    const auto append_key_records = [&]<bool with_count>()
    {
        using Record = StagedKeyRecord<RecordKey, with_count>;
        for (size_t i = 0; i < total; ++i)
        {
            prefetch_append(i);
            const UInt64 hash = hashes[i];
            const size_t partition = layout.partitionOf(hash);
            const size_t key_size = key_size_of(i);
            char * record = partitions.append(partition, Record::bytes(key_size));
            Record::writeHeader(record, hash, with_count ? adaptive.miss_multiplicities[i] : 0, key_size);
            write_key(i, key_size, record + Record::key_offset);
            zeroStagedRecordPadding(record + Record::key_offset + key_size);
            key_bytes += key_size;
        }
    };

    if (counts_only)
    {
        append_key_records.template operator()<true>();
    }
    else if (!params.aggregates_size)
    {
        append_key_records.template operator()<false>();
    }
    else
    {
        const auto & argument_layout = *adaptive_argument_layout;
        const size_t fixed_argument_bytes = argument_layout.fixed_bytes;

        /// The arguments are staged in the form the drain rebuilds and the instructions consume: the representation
        /// wrappers stripped recursively, then `LowCardinality` (see `buildAdaptiveArgumentLayout`).
        Columns arguments(argument_layout.num_positions);
        const auto normalize_argument = [&](size_t position) -> const IColumn &
        {
            arguments[position] = recursiveRemoveLowCardinality(columns[position]->convertToFullIfWrapped());
            return *arguments[position];
        };

        for (const auto & field : argument_layout.fixed_fields)
            normalize_argument(field.position);

        std::vector<const IColumn *> variable_sources;
        variable_sources.reserve(argument_layout.variable_fields.size());
        for (const auto & field : argument_layout.variable_fields)
            variable_sources.push_back(&normalize_argument(field.position));

        bool fixed_stride = false;
        if constexpr (!adaptive_key_stages_bytes<RecordKey>)
            fixed_stride = variable_sources.empty();

        /// A value whose serialized size cannot be computed in advance is serialized into a scratch arena first and
        /// copied from there. The values of one row form one contiguous range, which is rolled back once the record is
        /// written, so the scratch copies do not accumulate until the spill check. The arena is separate from
        /// `scratch_pool`, where the key holders of `write_key` allocate and roll back in between.
        const IColumn::SerializationSettings serialization_settings;
        std::vector<std::optional<size_t>> serialized_in_scratch(variable_sources.size());
        std::optional<Arena> argument_scratch;

        std::array<char *, adaptive_argument_staging_batch_rows> argument_rows{};
        for (size_t batch_begin = 0; batch_begin < total; batch_begin += argument_rows.size())
        {
            const size_t batch_end = std::min(total, batch_begin + argument_rows.size());
            if (fixed_stride)
            {
                using Record = StagedFixedArgumentRecord<RecordKey>;
                const size_t bytes = Record::bytes(fixed_argument_bytes);
                for (size_t i = batch_begin; i < batch_end; ++i)
                {
                    prefetch_append(i);
                    const size_t partition = layout.partitionOf(hashes[i]);
                    char * record = partitions.append(partition, bytes);
                    write_key(i, sizeof(RecordKey), record + Record::key_offset);
                    argument_rows[i - batch_begin] = record + Record::arguments_offset;
                }
                key_bytes += (batch_end - batch_begin) * sizeof(RecordKey);
            }
            else
            {
                for (size_t i = batch_begin; i < batch_end; ++i)
                {
                    prefetch_append(i);
                    const UInt64 hash = hashes[i];
                    const size_t partition = layout.partitionOf(hash);
                    const size_t key_size = key_size_of(i);
                    const size_t row = source_rows[i];

                    size_t variable_bytes = 0;
                    const char * scratch_begin = nullptr;
                    size_t scratch_bytes = 0;
                    for (size_t j = 0; j < variable_sources.size(); ++j)
                    {
                        if (const auto size = variable_sources[j]->getSerializedValueSize(row, &serialization_settings))
                        {
                            serialized_in_scratch[j].reset();
                            variable_bytes += *size;
                        }
                        else
                        {
                            /// Continuing the range may move it, so only the sizes are kept, and the values are read
                            /// from `scratch_begin` afterwards.
                            if (!argument_scratch)
                                argument_scratch.emplace();
                            const size_t size_of_value
                                = variable_sources[j]->serializeValueIntoArena(row, *argument_scratch, scratch_begin, &serialization_settings).size();
                            serialized_in_scratch[j] = size_of_value;
                            variable_bytes += size_of_value;
                            scratch_bytes += size_of_value;
                        }
                    }

                    const size_t key_offset = StagedArgumentRecord::keyOffset(fixed_argument_bytes);
                    const size_t bytes = alignStagedRecord(key_offset + key_size + variable_bytes);
                    char * record = partitions.append(partition, bytes);
                    StagedArgumentRecord::writeHeader(record, hash, bytes, key_size);

                    char * fixed = record + StagedArgumentRecord::header_bytes;
                    argument_rows[i - batch_begin] = fixed;

                    /// The key goes before the variable-size arguments, which overwrite whatever its copy
                    /// scribbled past it.
                    char * key = record + key_offset;
                    write_key(i, key_size, key);

                    char * variable = key + key_size;
                    const char * scratch = scratch_begin;
                    for (size_t j = 0; j < variable_sources.size(); ++j)
                    {
                        if (const auto & serialized_size = serialized_in_scratch[j])
                        {
                            memcpy(variable, scratch, *serialized_size);
                            variable += *serialized_size;
                            scratch += *serialized_size;
                        }
                        else
                            variable = variable_sources[j]->serializeValueIntoMemory(row, variable, &serialization_settings);
                    }
                    zeroStagedRecordPadding(variable);
                    if (scratch_bytes)
                        argument_scratch->rollback(scratch_bytes);

                    key_bytes += key_size;
                    variable_argument_bytes += variable_bytes;
                }
            }

            const size_t batch_rows = batch_end - batch_begin;
            for (const auto & field : argument_layout.fixed_fields)
                RowDataStore::gatherFieldToRows(
                    *arguments[field.position],
                    {source_rows + batch_begin, batch_rows},
                    {argument_rows.data(), batch_rows},
                    field.offset);
        }
    }

    /// The rows behind the records go to the count bins of the top-K pruning: a count record carries
    /// its combined multiplicity.
    if (adaptive.count_bins)
    {
        UInt16 * bins = adaptive.count_bins.get();
        for (size_t i = 0; i < total; ++i)
            addToCountBin(bins[adaptiveCountBin(hashes[i])], counts_only ? adaptive.miss_multiplicities[i] : 1);
    }

    adaptive.total_staged_records += total;

    /// The thaw evidence of the thread (see `adaptiveStagingRepeats`). A batch updates:
    ///
    /// - `staged_records` grows by the batch's record count.
    /// - `staged_bytes` grows by the batch's estimated footprint, computed below as
    ///   `batch_bytes`. It counts the key bytes as the kernel staged them, the variable-width
    ///   aggregate arguments at their serialized sizes, and the per-record bookkeeping (the
    ///   eight-byte routing hash, plus eight bytes of key size and padding only for byte-staged
    ///   keys). A column read by several aggregates is staged once, so it is counted once; a
    ///   count batch stages the combined multiplicity instead of arguments.
    ///   Variable-width arguments count in full because staging such a value pays real work
    ///   at every step. The record copies it out of the block, the record pins that memory
    ///   until the merge drains it, and updating the aggregate state from it copies the value
    ///   once more (a string min keeps its own copy of the winning value). A repeated key pays
    ///   all of that on every occurrence, where an unfrozen table would have paid a single
    ///   in-place state update, so each repeat of a heavy value is genuine waste.
    ///   Fixed-width arguments also occupy memory until the drain, and rebuilding their dense
    ///   columns copies them again before the ordinary batch executor consumes them. A repeated
    ///   key pays these copies on each occurrence, while an ordinary table updates its states in
    ///   place. Their fixed area is counted by `adaptiveStagingWastes` from the record count and
    ///   `AdaptiveArgumentLayout::fixed_bytes`. The comparison uses repetition within this producer,
    ///   since equal keys in different producers would also occupy separate ordinary tables. A
    ///   stream of five `UInt64` arguments at global repeat 10 can therefore remain profitable when
    ///   its local streams repeat little.
    /// - The sample receives routing hashes matching `hash & 0xFF == 0`. Each distinct key has the
    ///   same sampling probability, and `distinct_sampled_hashes` collapses its repeats onto one entry.
    ///   For a growing set aggregate, a bounded sketch also counts distinct argument tuples of these
    ///   keys, including the key in their hash because equal arguments in different groups occupy
    ///   separate states.
    if (adaptiveMayThaw(*adaptive.session))
    {
        chassert(!adaptive_state_bytes_per_distinct_input || adaptive_distinct_input_aggregate < aggregates_positions.size());
        size_t batch_bytes = key_bytes + (counts_only ? total * sizeof(UInt32) : variable_argument_bytes);
        batch_bytes += total * (sizeof(UInt64) + (adaptive_key_stages_bytes<RecordKey> ? sizeof(UInt64) : 0));

        auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
        frozen.staged_records += total;
        frozen.staged_bytes += batch_bytes;
        for (size_t i = 0; i < total; ++i)
        {
            const auto hash = hashes[i];
            if ((hash & adaptive_thaw_sample_mask) == 0)
            {
                frozen.distinct_sampled_hashes.insert(hash);
                if (adaptive_state_bytes_per_distinct_input)
                {
                    SipHash input_hash;
                    input_hash.update(hash);
                    for (const auto position : aggregates_positions[adaptive_distinct_input_aggregate])
                        columns[position]->updateHashWithValue(source_rows[i], input_hash);
                    frozen.addSampledInputHash(input_hash.get64());
                }
            }
        }
    }

    adaptive.miss_source_rows.clear();
    adaptive.miss_hashes.clear();
    adaptive.miss_key_sizes.clear();
    adaptive.miss_keys.clear();
    adaptive.miss_multiplicities.clear();

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedRecords, total);
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationStagedBytes, key_bytes);
}

}
