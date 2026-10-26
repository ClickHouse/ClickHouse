/// The merge kernels of the adaptive aggregation: the partition-wise merge of the staged records with the source
/// tables, and the conversion of each bucket. They are member templates of `Aggregator` (defined here rather than in
/// `Aggregator.cpp`, following `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <bit>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CacheLine.h>
#include <Common/FailPoint.h>
#include <Common/assert_cast.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
#include <Interpreters/AdaptiveAggregation/GrowingCountTopK.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/RowDataStore.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationDrainedRecords;
    extern const Event AdaptiveAggregationMergeUnits;
    extern const Event AdaptiveAggregationPrunedUnits;
    extern const Event AdaptiveAggregationPrunedRecords;
    extern const Event AdaptiveAggregationCountFirstUnits;
    extern const Event AdaptiveAggregationBucketsRetired;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char adaptive_aggregation_drain_create_states_throw[];
}

namespace
{
    /// Applies `f` to the two-level method the variant currently holds. The adaptive merge
    /// only ever works on two-level variants, so any other type is a logical error.
    template <typename F>
    void visitTwoLevelVariant(DB::AggregatedDataVariants & variants, F && f)
    {
#define M(NAME) \
    else if (variants.type == DB::AggregatedDataVariants::Type::NAME) \
        f(*variants.NAME);

        if (false) {} /// NOLINT
        APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
        else
            throw DB::Exception(
                DB::ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT, "Unknown aggregated data variant in the adaptive merge.");
    }

    /// The key holder (see `keyHolderGetKey`) of a staged key read back by `readStagedKey`. A string-like key points
    /// into its record, which the merge frees only after converting the unit. With `copy_keys`, the record is in a
    /// block read back from a spill, which the next block overwrites, so the holder copies the key into `arena` when
    /// a table keeps it.
    template <bool copy_keys, typename Key>
    auto ALWAYS_INLINE stagedKeyHolder(const Key & key, [[maybe_unused]] DB::Arena * arena)
    {
        if constexpr (copy_keys && std::is_same_v<Key, std::string_view>)
            return DB::ArenaKeyHolder{key, *arena};
        else if constexpr (copy_keys && std::is_same_v<Key, PackedStringRef>)
            return DB::ArenaPackedStringHolder{key, *arena};
        else
            return key;
    }

    /// Emplaces a staged key into `table` with its routing hash, copying it into `arena` if `copy_keys` (see
    /// `stagedKeyHolder`).
    template <typename Key, bool copy_keys, typename Table>
    void ALWAYS_INLINE emplaceStagedKey(
        Table & table,
        DB::Arena * arena,
        const char * key_pos,
        size_t key_size,
        size_t routing_hash,
        typename Table::LookupResult & it,
        bool & inserted)
    {
        table.emplace(stagedKeyHolder<copy_keys>(readStagedKey<Key>(key_pos, key_size, routing_hash), arena), it, inserted, routing_hash);
    }

    /// Emplaces the key of a source table's cell into `table` with the cell's hash. The key of a string table is a view
    /// its iteration handed out (see `forEachMappedCellWithHash`), which goes in with `emplaceIteratedKey`.
    template <typename Table, typename Key>
    void ALWAYS_INLINE emplaceSourceKey(Table & table, const Key & key, typename Table::LookupResult & it, bool & inserted, size_t hash)
    {
        if constexpr (requires { table.emplaceIteratedKey(key, it, inserted, hash); })
            table.emplaceIteratedKey(key, it, inserted, hash);
        else
            table.emplace(key, it, inserted, hash);
    }

    /// Prefetches the table slot of a staged record ahead of its emplace: hash-organized tables
    /// by the routing hash, string tables from the key bytes and the hash.
    template <typename Key, typename Table>
    void ALWAYS_INLINE prefetchStagedKey(Table & table, const char * key_pos, size_t key_size, size_t routing_hash)
    {
        if constexpr (requires { table.prefetchByHash(routing_hash); })
            table.prefetchByHash(routing_hash);
        else if constexpr (std::is_same_v<Key, std::string_view>)
            table.prefetch(std::string_view(key_pos, key_size), routing_hash);
    }

    /// Whether a walk over staged records prefetches the table slots of the records ahead: into a table that has outgrown
    /// the cache, or for string keys, whose emplace compares key bytes behind the slot. Prefetching a cache-resident slot
    /// is pure overhead.
    template <typename Key, typename Table>
    bool prefetchesStagedKeys(const Table & table)
    {
        return DB::adaptive_key_stages_bytes<Key> || table.getBufferSizeInBytes() > DB::adaptive_drain_prefetch_min_table_bytes;
    }

    /// The filters of a walk over staged records (see `walkStagedRecords`), which take a record by the routing hash of its
    /// key: every record, or the records of the count bins that the top-K pruning keeps alive.
    struct AllStagedRecords
    {
        static constexpr bool takes(UInt64) { return true; }
    };

    struct AliveBinRecords
    {
        const bool * alive_bins;

        bool ALWAYS_INLINE takes(UInt64 hash) const { return alive_bins[DB::bucketCountBin(hash)]; }
    };

    /// The filter of the best groups of a count-first unit: their hashes, sorted, and a bitset on 12 bits of them. A
    /// walk tests every record, so the bitset answers for nearly all of them with one predictable branch, before the
    /// search that would mispredict on random hashes. A group that shares the hash of a best one passes as well, and the
    /// conversion ranks it by its count with the others.
    class BestHashFilter
    {
    public:
        /// Sorts `hashes_`, which must outlive the filter.
        explicit BestHashFilter(DB::PaddedPODArray<UInt64> & hashes_)
            : hashes(hashes_)
        {
            std::sort(hashes_.begin(), hashes_.end());
            for (const UInt64 hash : hashes)
                bits[(hash >> 6) & 63] |= UInt64{1} << (hash & 63);
        }

        bool ALWAYS_INLINE contains(UInt64 hash) const
        {
            return ((bits[(hash >> 6) & 63] >> (hash & 63)) & 1) && std::binary_search(hashes.begin(), hashes.end(), hash);
        }

        bool ALWAYS_INLINE takes(UInt64 hash) const { return contains(hash); }

    private:
        const DB::PaddedPODArray<UInt64> & hashes;
        std::array<UInt64, 64> bits{};
    };

    /// How many records a walk took and how many it skipped.
    struct StagedRecordWalk
    {
        size_t taken = 0;
        size_t skipped = 0;
    };

    /// Walks the records of `ranges`, written in `format`, in order and calls `apply(record, hash)` for every record whose
    /// routing hash `filter` takes. With `prefetch`, which the caller decides once for the walk, it prefetches the table
    /// slot of the taken record `adaptive_drain_prefetch_look_ahead` positions ahead. The format, the filter and `apply`
    /// run once per record, so all of them are inlined into the loop.
    template <typename Key, typename Format, typename Table, typename Filter, typename Apply>
    StagedRecordWalk ALWAYS_INLINE walkStagedRecords(
        Format format, Table & table, const DB::AdaptiveRecordRanges & ranges, const Filter & filter, bool prefetch, Apply && apply)
    {
        StagedRecordWalk walk;
        const auto walk_ranges = [&]<bool with_prefetch>() ALWAYS_INLINE
        {
            /// The prefetch cursor runs the look-ahead distance in front of the walk and crosses from one range to the
            /// next as the walk does: a partition's ranges are as small as a first chunk of a few kilobytes, so
            /// restarting the distance in every range would leave a good share of the records unprefetched.
            [[maybe_unused]] size_t ahead_range = 0;
            [[maybe_unused]] const char * ahead = nullptr;
            [[maybe_unused]] const char * ahead_end = nullptr;
            [[maybe_unused]] const auto prefetch_next = [&]() ALWAYS_INLINE
            {
                while (ahead == ahead_end)
                {
                    if (ahead_range == ranges.size())
                        return;
                    ahead = ranges[ahead_range].data();
                    ahead_end = ahead + ranges[ahead_range].size();
                    ++ahead_range;
                    /// A range is cold until the cursor reads its records, and the hardware prefetchers take up a new
                    /// stream only after its first misses: the following range, up to a page, is requested into L2 as
                    /// the cursor enters this one.
                    if (ahead_range < ranges.size())
                    {
                        const char * next = ranges[ahead_range].data();
                        const char * const next_end
                            = next + std::min<size_t>(ranges[ahead_range].size(), DB::adaptive_drain_range_prefetch_bytes);
                        for (; next < next_end; next += CH_CACHE_LINE_SIZE)
                            __builtin_prefetch(next, /*rw=*/0, /*locality=*/2);
                    }
                }
                const UInt64 hash = format.hash(table, ahead);
                if (filter.takes(hash))
                {
                    const auto [key_pos, key_size] = format.key(ahead);
                    prefetchStagedKey<Key>(table, key_pos, key_size, hash);
                }
                ahead += format.bytes(ahead);
            };
            if constexpr (with_prefetch)
                for (size_t i = 0; i < DB::adaptive_drain_prefetch_look_ahead; ++i)
                    prefetch_next();

            for (const auto & range : ranges)
            {
                const char * record = range.data();
                const char * const end = range.data() + range.size();
                while (record < end)
                {
                    if constexpr (with_prefetch)
                        prefetch_next();
                    const UInt64 hash = format.hash(table, record);
                    if (filter.takes(hash))
                    {
                        apply(record, hash);
                        ++walk.taken;
                    }
                    else
                    {
                        ++walk.skipped;
                    }
                    record += format.bytes(record);
                }
            }
        };
        if (prefetch)
            walk_ranges.template operator()<true>();
        else
            walk_ranges.template operator()<false>();
        return walk;
    }

    /// Visits every cell of a map table with its key as the table stores it, its mapped value and its hash: from the
    /// iterator, which reads a saved hash instead of rehashing, where the table has one, and through
    /// `forEachValueWithHash` for the string table, whose sub-maps share no iterator, and which hands out the short
    /// keys as views that only `emplaceIteratedKey` may read.
    template <typename Table, typename Callback>
    void forEachMappedCellWithHash(Table & table, Callback && callback)
    {
        if constexpr (requires { table.begin(); })
        {
            for (auto it = table.begin(), end = table.end(); it != end; ++it)
                callback(Table::cell_type::getKey(it->getValue()), it->getMapped(), it.getHash());
        }
        else
        {
            table.forEachValueWithHash(callback);
        }
    }

    /// Visits every cell of a map table like `forEachMappedCellWithHash`, but hands out a callable that returns the
    /// cell's hash, valid during the call, for a caller that needs the hashes of a few cells: an iterator over a table
    /// without saved hashes rehashes the key.
    template <typename Table, typename Callback>
    void forEachMappedCellWithHashOnDemand(Table & table, Callback && callback)
    {
        if constexpr (requires { table.begin(); })
        {
            for (auto it = table.begin(), end = table.end(); it != end; ++it)
                callback(it->getMapped(), [&it] { return it.getHash(); });
        }
        else
        {
            table.forEachValueWithHash([&](const auto &, auto & mapped, size_t hash) { callback(mapped, [hash] { return hash; }); });
        }
    }

    /// The staged records of the bucket that a merge task merges, partition by partition: the producers' chunks of a
    /// partition and its spill stream (see `AdaptiveAggregationSession::SpilledPartition`). A merge unit takes the
    /// partitions [first_sub, end_sub): `forEachInMemoryPartition` hands out their records in memory, valid until
    /// `release`, and `forEachSpilledBlock` reads their spilled records back a block at a time, once per pass of the
    /// caller, each block valid only during its call. `release` frees both.
    class AdaptiveBucketRecords
    {
    public:
        AdaptiveBucketRecords(DB::AdaptiveAggregationSession & session_, size_t bucket)
            : session(session_)
            , first_partition(session_.layout.firstPartition(bucket))
        {
        }

        /// The record count of partition `sub` of the bucket, in the producers' chunks and in its spill stream.
        size_t recordsOf(size_t sub) const
        {
            size_t records = spilledOf(sub).recordCount();
            for (const auto & producer : session.producer_buffers)
                records += producer->recordsOf(first_partition + sub);
            return records;
        }

        /// Calls `f(ranges)` with the records in memory of each partition in [first_sub, end_sub), its chunks of every
        /// producer.
        template <typename F>
        void forEachInMemoryPartition(size_t first_sub, size_t end_sub, DB::AdaptiveRecordRanges & ranges, F && f) const
        {
            for (size_t sub = first_sub; sub < end_sub; ++sub)
            {
                ranges.clear();
                for (const auto & producer : session.producer_buffers)
                    producer->forEachChunk(first_partition + sub, [&](std::string_view records) { ranges.push_back(records); });
                f(ranges);
            }
        }

        /// Calls `f(ranges)` with the records of each block of the spill streams of the partitions in [first_sub,
        /// end_sub), read back a block at a time (see `SpilledPartition::readBack`), until a call returns false. Out of
        /// line, so the code of the units that did not spill stays as it is.
        template <typename F>
        void NO_INLINE forEachSpilledBlock(size_t first_sub, size_t end_sub, DB::AdaptiveRecordRanges & ranges, F && f)
        {
            bool keep_reading = true;
            for (size_t sub = first_sub; sub < end_sub && keep_reading; ++sub)
            {
                spilledOf(sub).readBack([&](const DB::PaddedPODArray<char> & block)
                {
                    ranges.clear();
                    ranges.emplace_back(block.data(), block.size());
                    keep_reading = f(ranges);
                    return keep_reading;
                });
            }
        }

        /// Frees the records of partition `sub` of the bucket: the producers' chunks and its spill stream, which a unit
        /// that the top-K pruning skips leaves unread.
        void release(size_t sub)
        {
            for (const auto & producer : session.producer_buffers)
                producer->releasePartition(first_partition + sub);
            spilledOf(sub).release();
        }

    private:
        DB::AdaptiveAggregationSession::SpilledPartition & spilledOf(size_t sub) const
        {
            return session.spilled_partitions[first_partition + sub];
        }

        DB::AdaptiveAggregationSession & session;
        const size_t first_partition;
    };

    /// The same for a set table, whose cells hold only keys.
    template <typename Table, typename Callback>
    void forEachKeyCellWithHash(Table & table, Callback && callback)
    {
        for (auto it = table.begin(), end = table.end(); it != end; ++it)
            callback(Table::cell_type::getKey(it->getValue()), it.getHash());
    }

    /// Decodes the variable-size arguments of argument records of `format` into one column per variable field of
    /// `layout`, in the order of the records. The values of a record follow its key: a `String` as `writeStagedString`
    /// staged it, appended without a read buffer or a virtual call, and another value in its column's own serialization.
    template <typename Format>
    MutableColumns decodeStagedVariableArguments(
        const Format & format, const AdaptiveArgumentLayout & layout, const RowStorePointers & records)
    {
        MutableColumns columns;
        std::vector<ColumnString *> string_columns;
        columns.reserve(layout.variable_fields.size());
        string_columns.reserve(layout.variable_fields.size());
        for (const auto & field : layout.variable_fields)
        {
            auto column = field.type->createColumn();
            column->reserve(records.ptrs.size());
            string_columns.push_back(isStagedString(field) ? &assert_cast<ColumnString &>(*column) : nullptr);
            columns.push_back(std::move(column));
        }

        const IColumn::SerializationSettings serialization_settings;
        for (const char * record : records.ptrs)
        {
            const auto [key_pos, key_size] = format.key(record);
            const char * value = key_pos + key_size;
            for (size_t j = 0; j < columns.size(); ++j)
            {
                if (auto * string = string_columns[j])
                {
                    const std::string_view staged = readStagedString(value);
                    string->insertData(staged.data(), staged.size());
                }
                else
                {
                    ReadBufferFromMemory in(value, record + format.bytes(record) - value);
                    columns[j]->deserializeAndInsertFromArena(in, &serialization_settings);
                    value = in.position();
                }
            }
        }
        return columns;
    }

}

template <typename Method, bool copy_keys, typename Table, typename Filter>
size_t NO_INLINE Aggregator::drainAdaptivePartition(
    Table & table,
    Arena * arena,
    const AdaptiveRecordRanges & ranges,
    const Filter & filter,
    PaddedPODArray<AggregateDataPtr> & places,
    RowStorePointers & records) const
{
    using Key = AdaptiveRecordKey<Method>;
    const bool prefetch = prefetchesStagedKeys<Key>(table);
    StagedRecordWalk walk;
    visitStagedRecordFormat<Key>(adaptive_argument_layout.get(), is_simple_count, [&]<typename Record>(Record format) ALWAYS_INLINE
    {
        if constexpr (Record::with_count)
        {
            walk = walkStagedRecords<Key>(
                format,
                table,
                ranges,
                filter,
                prefetch,
                [&](const char * record, UInt64 hash) ALWAYS_INLINE
                {
                    typename Table::LookupResult it;
                    bool inserted = false;
                    const auto [key_pos, key_size] = format.key(record);
                    emplaceStagedKey<Key, copy_keys>(table, arena, key_pos, key_size, hash, it, inserted);
                    if constexpr (MapAggregationMethod<Method>)
                    {
                        if (inserted)
                            getInlineCountState(it->getMapped()) = Record::count(record);
                        else
                            getInlineCountState(it->getMapped()) += Record::count(record);
                    }
                });
        }
        else if constexpr (!Record::with_arguments)
        {
            /// A map method without aggregate functions still gives a new key a place, of no size.
            walk = walkStagedRecords<Key>(
                format,
                table,
                ranges,
                filter,
                prefetch,
                [&](const char * record, UInt64 hash) ALWAYS_INLINE
                {
                    typename Table::LookupResult it;
                    bool inserted = false;
                    const auto [key_pos, key_size] = format.key(record);
                    emplaceStagedKey<Key, copy_keys>(table, arena, key_pos, key_size, hash, it, inserted);
                    if constexpr (MapAggregationMethod<Method>)
                    {
                        if (inserted)
                        {
                            it->getMapped() = nullptr;
                            /// Stands for the creation of the states failing, as on a memory limit: the key stays
                            /// in the table with a null state.
                            fiu_do_on(FailPoints::adaptive_aggregation_drain_create_states_throw,
                            {
                                throw Exception(ErrorCodes::FAULT_INJECTED, "Injected fault while creating the states of a merged key");
                            });
                            AggregateDataPtr place = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                            createAggregateStates(place);
                            it->getMapped() = place;
                        }
                    }
                });
        }
        else if constexpr (MapAggregationMethod<Method>)
        {
            bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
            use_compiled_functions = compiled_aggregate_functions_holder != nullptr;
#endif

            places.clear();
            records.ptrs.clear();
            walk = walkStagedRecords<Key>(
                format,
                table,
                ranges,
                filter,
                prefetch,
                [&](const char * record, UInt64 hash) ALWAYS_INLINE
                {
                    typename Table::LookupResult it;
                    bool inserted = false;
                    const auto [key_pos, key_size] = format.key(record);
                    emplaceStagedKey<Key, copy_keys>(table, arena, key_pos, key_size, hash, it, inserted);
                    if (inserted)
                    {
                        it->getMapped() = nullptr;
                        /// Stands for the creation of the states failing, as on a memory limit: the key stays
                        /// in the table with a null state.
                        fiu_do_on(FailPoints::adaptive_aggregation_drain_create_states_throw,
                        {
                            throw Exception(ErrorCodes::FAULT_INJECTED, "Injected fault while creating the states of a merged key");
                        });
                        AggregateDataPtr place = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(place, use_compiled_functions);
                        it->getMapped() = place;
                    }
                    places.push_back(it->getMapped());
                    records.ptrs.push_back(record);
                });

            /// A numeric key that is also an argument is read from the key's bytes in the record.
            chassert(!adaptive_argument_layout->key_field
                || adaptive_argument_layout->key_field->type->getSizeOfValueInMemory() == sizeof(Key));
            if (!places.empty())
                executeStagedArgumentRecords(format, arena, places, records, use_compiled_functions);
        }
    });

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationDrainedRecords, walk.taken);
    return walk.skipped;
}

template <typename Method, bool copy_keys, typename Table, typename Filter, typename UpdateCandidates>
requires MapAggregationMethod<Method>
size_t NO_INLINE Aggregator::countPartitionGroups(
    Table & counting_table,
    Arena * arena,
    const AdaptiveRecordRanges & ranges,
    const Filter & filter,
    const UpdateCandidates & update_candidates) const
{
    /// A count-first unit ranks by a count among other aggregates, so its records carry arguments.
    chassert(adaptive_argument_layout && !is_simple_count);
    using Key = AdaptiveRecordKey<Method>;
    const bool prefetch = prefetchesStagedKeys<Key>(counting_table);
    size_t skipped = 0;
    visitStagedRecordFormat<Key>(adaptive_argument_layout.get(), is_simple_count, [&]<typename Record>(Record format) ALWAYS_INLINE
    {
        if constexpr (Record::with_arguments)
        {
            skipped = walkStagedRecords<Key>(
                format,
                counting_table,
                ranges,
                filter,
                prefetch,
                [&](const char * record, UInt64 hash) ALWAYS_INLINE
                {
                    typename Table::LookupResult it;
                    bool inserted = false;
                    const auto [key_pos, key_size] = format.key(record);
                    const Key key = readStagedKey<Key>(key_pos, key_size, hash);
                    counting_table.emplace(stagedKeyHolder<copy_keys>(key, arena), it, inserted, hash);
                    UInt64 & group_count = getInlineCountState(it->getMapped());
                    if (inserted)
                        group_count = 1;
                    else
                        ++group_count;
                    update_candidates(group_count, key, hash, std::bool_constant<copy_keys>{});
                }).skipped;
        }
    });
    return skipped;
}

ALWAYS_INLINE void Aggregator::discardAdaptiveSourceState(AggregateDataPtr & place) const
{
    if (!is_simple_count && !all_aggregates_has_trivial_destructor)
        for (size_t i = 0; i < params.aggregates_size; ++i)
            aggregate_functions[i]->destroy(place + offsets_of_aggregate_states[i]);
    place = nullptr;
}

template <typename Method, typename Table, typename Cells, typename Filter>
void Aggregator::adoptSourceCells(Table & table, Cells & cells, const Filter & filter, AdaptiveMergeTask & task) const
{
    task.places.clear();
    task.source_slots.clear();
    for (auto & cell : cells)
    {
        if (!filter.takes(cell.hash))
        {
            if constexpr (MapAggregationMethod<Method>)
                discardAdaptiveSourceState(*cell.mapped);
            continue;
        }

        typename Table::LookupResult it;
        bool inserted = false;
        emplaceSourceKey(table, cell.key, it, inserted, cell.hash);
        if constexpr (MapAggregationMethod<Method>)
        {
            AggregateDataPtr & source_place = *cell.mapped;
            if (is_simple_count)
            {
                if (inserted)
                    getInlineCountState(it->getMapped()) = getInlineCountState(source_place);
                else
                    getInlineCountState(it->getMapped()) += getInlineCountState(source_place);
                source_place = nullptr;
            }
            else if (inserted)
            {
                it->getMapped() = source_place;
                source_place = nullptr;
            }
            else
            {
                task.places.push_back(it->getMapped());
                task.source_slots.push_back(&source_place);
            }
        }
    }
}

template <typename Method, typename Table, typename Cells, typename TopK, typename ForEachInMemoryPartition, typename ForEachSpilledBlock>
size_t Aggregator::selectCountFirstBestGroups(
    Table & counting_table,
    const Cells & cells,
    const bool * alive_bins,
    TopK * top_k,
    const AdaptiveTopKPruning * pruning,
    const ForEachInMemoryPartition & for_each_in_memory_partition,
    const ForEachSpilledBlock & for_each_spilled_block,
    Arena * arena,
    AdaptiveMergeTask & task,
    size_t & pruned_records) const
{
    using Key = AdaptiveRecordKey<Method>;
    const size_t rank_offset = offsets_of_aggregate_states[params.bucket_top_k_rank_index];

    /// The groups are counted in the table, each count kept in the mapped value as a lone `count()` keeps it: a source
    /// cell adds its count state, a record one row. With a descending order, the top takes every grown count and keeps
    /// its indices in the high bits of the counts.
    if (top_k)
        top_k->clear();
    for (const auto & cell : cells)
    {
        if (alive_bins && !alive_bins[bucketCountBin(cell.hash)])
            continue;
        typename Table::LookupResult it;
        bool inserted = false;
        emplaceSourceKey(counting_table, cell.key, it, inserted, cell.hash);
        const UInt64 count = getCountState(*cell.mapped + rank_offset);
        UInt64 & group_count = getInlineCountState(it->getMapped());
        if (inserted)
            group_count = count;
        else
            group_count += count;
        if (top_k)
            top_k->template update</*copy_key=*/false>(group_count, cell.key, cell.hash);
    }
    const auto update_candidates
        = [&]<bool copy_key>(UInt64 & group_count, const auto & key, UInt64 hash, std::bool_constant<copy_key>) ALWAYS_INLINE
    {
        if (top_k)
            top_k->template update<copy_key>(group_count, key, hash);
    };
    /// The records in memory, then those of the spill streams, whose blocks go after their calls: the counting table
    /// copies the keys it keeps into `arena`, and the top keeps copies of its own (see `GrowingCountTopK`).
    const auto count_records = [&](const auto & filter)
    {
        for_each_in_memory_partition([&](const AdaptiveRecordRanges & ranges)
        {
            pruned_records += countPartitionGroups<Method, /*copy_keys=*/false>(counting_table, arena, ranges, filter, update_candidates);
        });
        for_each_spilled_block([&](const AdaptiveRecordRanges & ranges)
        {
            pruned_records += countPartitionGroups<Method, adaptive_key_stages_bytes<Key>>(
                counting_table, arena, ranges, filter, update_candidates);
        });
    };
    if (alive_bins != nullptr)
        count_records(AliveBinRecords{alive_bins});
    else
        count_records(AllStagedRecords{});

    /// The best groups by their counts, which are exact: the unit holds every record and source cell of its keys.
    auto & best_hashes = task.best_hashes;
    best_hashes.clear();
    if (top_k)
    {
        /// With the pruning, which serves a descending order only, a group counted below the threshold cannot reach
        /// the top.
        const UInt64 threshold = pruning ? pruning->currentThreshold() : 0;
        for (const auto & group : top_k->getGroups())
            if (group.count >= threshold)
                best_hashes.push_back(group.hash);
    }
    else
    {
        /// The smallest counts, selected by a pass over the counted groups. Only the cells that enter the heap are
        /// hashed; its front holds the largest count kept.
        auto & best = task.best_counts_and_hashes;
        best.clear();
        const auto by_count = [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; };
        forEachMappedCellWithHashOnDemand(
            counting_table,
            [&](AggregateDataPtr & mapped, const auto & hash_of)
            {
                const UInt64 count = getInlineCountState(mapped);
                if (best.size() < params.bucket_top_k)
                {
                    best.emplace_back(count, hash_of());
                    std::push_heap(best.begin(), best.end(), by_count);
                }
                else if (count < best.front().first)
                {
                    std::pop_heap(best.begin(), best.end(), by_count);
                    best.back() = {count, hash_of()};
                    std::push_heap(best.begin(), best.end(), by_count);
                }
            });
        for (const auto & [count, hash] : best)
            best_hashes.push_back(hash);
    }
    return counting_table.size();
}

template <typename Record>
void Aggregator::executeStagedArgumentRecords(
    const Record & format,
    Arena * arena,
    PaddedPODArray<AggregateDataPtr> & places,
    const RowStorePointers & records,
    bool use_compiled_functions) const
{
    const auto & argument_layout = *adaptive_argument_layout;
    const size_t rows = places.size();

    /// The arguments are rebuilt into dense columns while the records are hot, so the
    /// ordinary batch executor, and the compiled functions, apply to them as to a block.
    Columns arguments(argument_layout.num_positions);
    if (const auto & field = argument_layout.key_field)
    {
        auto column = field->type->createColumn();
        column->fillFromRowStorePtrs(field->type, records, format.key_offset, column->sizeOfValueIfFixed(), 0, rows);
        arguments[field->position] = std::move(column);
    }
    for (const auto & field : argument_layout.fixed_fields)
    {
        auto column = field.type->createColumn();
        column->fillFromRowStorePtrs(field.type, records, Record::fixed_arguments_offset + field.offset, field.size, 0, rows);
        arguments[field.position] = std::move(column);
    }

    /// Only the records with a variable size carry variable-size arguments, after the key.
    if (!argument_layout.variable_fields.empty())
    {
        auto variable_columns = decodeStagedVariableArguments(format, argument_layout, records);
        for (size_t j = 0; j < variable_columns.size(); ++j)
            arguments[argument_layout.variable_fields[j].position] = std::move(variable_columns[j]);
    }

    executeStagedArguments(arena, places, arguments, use_compiled_functions);
}

void Aggregator::executeStagedArguments(
    Arena * arena, PaddedPODArray<AggregateDataPtr> & places, const Columns & arguments, bool use_compiled_functions) const
{
    const size_t rows = places.size();
    AggregateColumns aggregate_columns(params.aggregates_size);
    AggregateFunctionInstructions instructions(params.aggregates_size + 1);
    NestedColumnsHolder nested_columns_holder;
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        aggregate_columns[i].resize(aggregates_positions[i].size());
        for (size_t j = 0; j < aggregate_columns[i].size(); ++j)
            aggregate_columns[i][j] = arguments[aggregates_positions[i][j]].get();
        buildAggregateFunctionInstruction(
            i, /*has_sparse_arguments=*/false, aggregate_columns, instructions, nested_columns_holder);
    }
    instructions[params.aggregates_size].that = nullptr;

    executeAggregateInstructions(
        arena,
        0,
        rows,
        instructions.data(),
        places.data(),
        /*key_start=*/0,
        /*has_only_one_value_since_last_reset=*/false,
        /*all_keys_are_const=*/false,
        /*all_places_are_non_null=*/true,
        use_compiled_functions);
}

ALWAYS_INLINE UInt64 Aggregator::adaptiveRankCount(AggregateDataPtr & mapped, IColumn * scratch, Arena * arena) const
{
    switch (bucket_top_k_rank)
    {
        case BucketTopKRank::InlineCount:
            return getInlineCountState(mapped);
        case BucketTopKRank::CountState:
            return getCountState(mapped + offsets_of_aggregate_states[params.bucket_top_k_rank_index]);
        case BucketTopKRank::FinalizedResult:
            return finalizeBucketTopKRank(mapped, *scratch, arena);
    }
}

size_t Aggregator::collectAdaptiveTableStatistics(AggregatedDataVariants & variants, AdaptiveTopKCountBins * bins) const
{
    size_t work = 0;
#define M(NAME) \
    else if (variants.type == AggregatedDataVariants::Type::NAME) \
        work = collectAdaptiveTableStatistics(*variants.NAME, variants.aggregates_pool, bins);

    if (variants.empty()) {} // NOLINT
    APPLY_FOR_VARIANTS_CONVERTIBLE_TO_TWO_LEVEL(M)
    APPLY_FOR_VARIANTS_TWO_LEVEL(M)
#undef M
    else
        throw Exception(
            ErrorCodes::UNKNOWN_AGGREGATED_DATA_VARIANT,
            "The adaptive aggregation cannot collect table statistics for variant {}", variants.getMethodName());
    return work;
}

template <typename Method>
size_t Aggregator::collectAdaptiveTableStatistics(Method & method, Arena * arena, AdaptiveTopKCountBins * bins) const
{
    size_t work = 0;
    if constexpr (MapAggregationMethod<Method>)
    {
        /// A `uniqExact` or `uniqExactIf` rank adds the distinct count of the cell: the distinct count of a merged group
        /// is at most the sum of those of the cells and records merged into it, so the bins bound it as they bound a row
        /// count.
        ColumnUInt64::MutablePtr scratch;
        if (bins && bucket_top_k_rank == BucketTopKRank::FinalizedResult)
            scratch = ColumnUInt64::create();
        const auto add = [&](auto & table)
        {
            forEachMappedCellWithHashOnDemand(
                table,
                [&](AggregateDataPtr & mapped, const auto & hash_of)
                {
                    if (bins)
                        bins->add(hash_of(), adaptiveRankCount(mapped, scratch.get(), arena));
                    for (size_t i : adaptive_parallel_merge_indices)
                        work += aggregate_functions[i]->getEstimatedMergeWork(mapped + offsets_of_aggregate_states[i]);
                });
        };
        if constexpr (requires { method.data.impls; })
        {
            for (auto & impl : method.data.impls)
                add(impl);
        }
        else
        {
            add(method.data);
        }
    }
    return work;
}

template <typename Method>
void NO_INLINE Aggregator::addMergeCountedTablesToBounds(
    ManyAggregatedDataVariants & data, Int32 bucket, const AdaptiveTopKPruning & pruning, std::span<UInt64> bounds, Arena * arena) const
{
    if constexpr (MapAggregationMethod<Method>)
    {
        ColumnUInt64::MutablePtr scratch;
        for (size_t i = 1; i < data.size(); ++i)
        {
            if (!pruning.isMergeCounted(*data[i]))
                continue;
            if (!scratch && bucket_top_k_rank == BucketTopKRank::FinalizedResult)
                scratch = ColumnUInt64::create();
            forEachMappedCellWithHash(
                getDataVariant<Method>(*data[i]).data.impls[bucket],
                [&](const auto &, AggregateDataPtr & mapped, size_t hash)
                { AdaptiveTopKPruning::addToBounds(bounds, hash, adaptiveRankCount(mapped, scratch.get(), arena)); });
        }
    }
}

Aggregator::AggregatedChunks Aggregator::mergeAndConvertAdaptiveBucket(
    ManyAggregatedDataVariants & data,
    AdaptiveAggregationSession & session,
    AdaptiveMergeTask & task,
    bool final,
    Int32 bucket,
    std::atomic<bool> & is_cancelled,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater,
    size_t * full_group_count) const
{
    auto & dest = *data[0];
    AggregatedChunks chunks;
    visitTwoLevelVariant(
        dest,
        [&](auto & method)
        {
            chunks = mergeAndConvertAdaptiveBucketImpl(
                dest, method, data, session, task, final, bucket, is_cancelled, updater, full_group_count);
        });
    return chunks;
}

template <typename Method>
Aggregator::AggregatedChunks Aggregator::mergeAndConvertAdaptiveBucketImpl(
    AggregatedDataVariants & dest,
    Method & dest_method,
    ManyAggregatedDataVariants & data,
    AdaptiveAggregationSession & session,
    AdaptiveMergeTask & task,
    bool final,
    Int32 bucket,
    std::atomic<bool> & is_cancelled,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater,
    size_t * full_group_count) const
{
    /// The task's table, emptied by the conversion of its previous bucket's last unit, moves into this bucket's slot:
    /// the slots are exclusively owned by the tasks that merge their buckets, and a retired bucket's slot is never
    /// read again. Insertion grows the table by its distinct keys, and conversion retains that capacity
    /// for the next unit or bucket. Reserving by the record count would oversize tables with repeated keys,
    /// increasing the cost of probing, scanning and clearing them.
    if (task.previous_bucket >= 0)
        std::swap(dest_method.data.impls[bucket], dest_method.data.impls[task.previous_bucket]);
    task.previous_bucket = bucket;

    using Table = std::decay_t<decltype(dest_method.data.impls[0])>;
    static_assert(std::decay_t<decltype(dest_method.data)>::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    auto & table = dest_method.data.impls[bucket];
    /// The unit being merged creates its states in the arena of the bucket's slot, which takes a fresh arena for every
    /// unit (see below).
    auto & arena_slot = dest.adaptive_merge_bucket_arenas[bucket];
    Arena * arena = arena_slot.get();

    const AdaptivePartitionLayout layout = session.layout;
    const size_t partitions_per_bucket = layout.partitionsPerBucket();

    AdaptiveBucketRecords staged(session, bucket);
    std::vector<size_t> partition_records(partitions_per_bucket);
    size_t bucket_records = 0;
    for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
    {
        partition_records[sub] = staged.recordsOf(sub);
        bucket_records += partition_records[sub];
    }

    size_t source_cells = 0;
    for (size_t i = 1; i < data.size(); ++i)
        source_cells += getDataVariant<Method>(*data[i]).data.impls[bucket].size();

    /// A unit is a run of partitions; the bucket splits into as many as give each unit about
    /// `adaptive_merge_unit_records` records and source cells, at most one per partition.
    const size_t units = std::min(
        partitions_per_bucket, std::bit_ceil(std::max<size_t>(1, (bucket_records + source_cells) / adaptive_merge_unit_records)));
    const size_t partitions_per_unit = partitions_per_bucket / units;
    const size_t unit_shift = std::countr_zero(partitions_per_unit);

    /// The sources' cells of the bucket, routed to their units by the same hash bits that partitioned the records.
    struct SourceCell
    {
        typename Table::key_type key;
        AggregateDataPtr * mapped;
        size_t hash;
    };
    std::vector<std::vector<SourceCell>> unit_cells(units);
    const auto unit_of = [&](size_t hash)
    {
        const size_t partition = layout.partitionOf(hash);
        chassert(layout.bucketOf(partition) == static_cast<size_t>(bucket));
        return layout.subOf(partition) >> unit_shift;
    };
    for (size_t i = 1; i < data.size(); ++i)
    {
        auto & source = getDataVariant<Method>(*data[i]).data.impls[bucket];
        if constexpr (MapAggregationMethod<Method>)
            forEachMappedCellWithHash(
                source, [&](const auto & key, AggregateDataPtr & mapped, size_t hash) { unit_cells[unit_of(hash)].push_back({key, &mapped, hash}); });
        else
            forEachKeyCellWithHash(source, [&](const auto & key, size_t hash) { unit_cells[unit_of(hash)].push_back({key, nullptr, hash}); });
    }

    /// With the top-K pruning, the bounds of the bucket's count bins (see `AdaptiveTopKPruning`), completed with the
    /// groups of the tables of the producers that never froze.
    AdaptiveTopKPruning * const pruning = session.top_k_pruning.get();
    AdaptiveTopKPruning::BucketBounds bin_bounds{};
    if (pruning)
    {
        bin_bounds = pruning->boundsForBucket(bucket);
        addMergeCountedTablesToBounds<Method>(data, bucket, *pruning, bin_bounds, arena);
    }
    AdaptiveTopKPruning::AliveBins alive{};
    size_t pruned_records = 0;

    /// A top-K by a count among other aggregates keeps only each unit's best groups by the count in the final
    /// conversion, so the other aggregate states of the rest of the groups would be built for nothing: a count-first
    /// unit counts its groups first and builds the states of its best groups only (see below). A merge that collects
    /// the statistics needs every group converted.
    const bool count_first = MapAggregationMethod<Method> && final && params.bucket_top_k
        && bucket_top_k_rank == BucketTopKRank::CountState && !updater;
    /// With a descending order, a count-first unit keeps its best groups up to date while it counts them. An ascending
    /// order cannot be kept this way, so a pass over the counted groups selects them.
    std::optional<GrowingCountTopK<typename Table::key_type, Params::max_bucket_top_k>> top_k;
    if (count_first && !params.bucket_top_k_ascending)
        top_k.emplace(params.bucket_top_k);
    /// A count-first unit keeps integer counts outside `AggregatedDataVariants`, whose cleanup treats mapped values
    /// as aggregate-state pointers. Its counting table takes the buffer retained from the previous bucket, while the
    /// bucket's own table holds the states of the best groups and remains valid during exception unwinding.
    Table counting_table;
    if (count_first)
        std::swap(table, counting_table);

    AggregatedChunks chunks;
    for (size_t unit = 0; unit < units; ++unit)
    {
        if (is_cancelled.load(std::memory_order_seq_cst))
            return chunks;

        const size_t unit_first_sub = unit * partitions_per_unit;
        const size_t unit_end_sub = unit_first_sub + partitions_per_unit;
        size_t unit_records = 0;
        for (size_t sub = unit_first_sub; sub < unit_end_sub; ++sub)
            unit_records += partition_records[sub];
        auto & cells = unit_cells[unit];
        if (!unit_records && cells.empty())
            continue;

        /// The unit's bins that can still hold a group of the top, at the threshold of the moment (see
        /// `AdaptiveTopKPruning::selectUnit`): a unit with none is skipped, its source cells dropped and its records freed
        /// unread; a unit with some drains only theirs.
        const bool * alive_bins = nullptr;
        if (pruning)
        {
            const auto selection = pruning->selectUnit(bin_bounds, unit, units, alive);
            if (selection == AdaptiveTopKPruning::UnitSelection::Skip)
            {
                if constexpr (MapAggregationMethod<Method>)
                    for (auto & cell : cells)
                        discardAdaptiveSourceState(*cell.mapped);
                for (size_t sub = unit_first_sub; sub < unit_end_sub; ++sub)
                    staged.release(sub);
                pruned_records += unit_records;
                ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPrunedUnits);
                continue;
            }
            if (selection == AdaptiveTopKPruning::UnitSelection::Filter)
                alive_bins = alive.data();
        }

        /// Calls `f` with the records in memory of each partition of the unit.
        const auto for_each_in_memory_partition = [&](auto && f)
        {
            staged.forEachInMemoryPartition(unit_first_sub, unit_end_sub, task.ranges, f);
        };

        /// Calls `f` with the records of each block of the spill streams of the unit's partitions, valid only during
        /// the call: whatever keeps a string-like key past it copies the key into the unit's arena, which lives until
        /// the unit is converted. The read ends once the query is cancelled.
        const auto for_each_spilled_block = [&](auto && f)
        {
            staged.forEachSpilledBlock(unit_first_sub, unit_end_sub, task.ranges, [&](const AdaptiveRecordRanges & ranges)
            {
                if (is_cancelled.load(std::memory_order_seq_cst))
                    return false;
                f(ranges);
                return true;
            });
        };

        /// Merges the source cells and the records of the unit that `filter` takes into the bucket's table: the records
        /// of the spill streams, then those in memory. A thread writes its staged records to the spill streams in the
        /// order it staged them and keeps only its latest ones in memory, so this order adds every thread's records to
        /// the states in the order the thread staged them, as the aggregation without staging adds each thread's rows:
        /// `groupArray` lists the values that a thread staged for a key in the order of that thread's rows, unless the
        /// thread thawed and took the key into its table. A function that needs the rows of each state in order does
        /// not take the adaptive path at all (see `AggregateFunctionProperties::requires_ordered_rows_per_state`). The
        /// cells go first: a key a source holds is adopted with its state, so the records of that key update the
        /// adopted state instead of creating one. The states of the other sources that hold the key merge into it last,
        /// as the aggregation without staging merges the states of the threads after adding their rows: a merged state
        /// can take a costlier form, such as the union of `uniqTheta`, which takes every later row through a sketch of
        /// its own. The merge keeps the addresses of the adopted states, which the records' insertions into the table
        /// do not move. Returns how many records `filter` skipped.
        const auto merge_unit = [&](const auto & filter)
        {
            adoptSourceCells<Method>(table, cells, filter, task);
            size_t skipped = 0;
            for_each_spilled_block([&](const AdaptiveRecordRanges & ranges)
            {
                skipped += drainAdaptivePartition<Method, adaptive_key_stages_bytes<AdaptiveRecordKey<Method>>>(
                    table, arena, ranges, filter, task.record_places, task.records);
            });
            for_each_in_memory_partition([&](const AdaptiveRecordRanges & ranges)
            {
                skipped += drainAdaptivePartition<Method, /*copy_keys=*/false>(
                    table, arena, ranges, filter, task.record_places, task.records);
            });
            if (!task.places.empty())
                mergeAdaptiveSourceStates(task.places, task.source_slots, task, session, arena, is_cancelled);
            return skipped;
        };

        /// The groups of the unit, which a count-first unit does not keep in its table.
        size_t unit_groups = 0;
        if (count_first)
        {
            if constexpr (MapAggregationMethod<Method>)
            {
                /// The first pass counts the unit's groups and selects its best groups. The second builds the states of
                /// the best groups in the bucket's table for the conversion, as the ordinary merge does, and the other
                /// source cells go with their states. Each pass reads the spill streams of the unit's partitions back.
                unit_groups = selectCountFirstBestGroups<Method>(
                    counting_table,
                    cells,
                    alive_bins,
                    top_k ? &*top_k : nullptr,
                    pruning,
                    for_each_in_memory_partition,
                    for_each_spilled_block,
                    arena,
                    task,
                    pruned_records);
                counting_table.clear();
                merge_unit(BestHashFilter(task.best_hashes));
                ProfileEvents::increment(ProfileEvents::AdaptiveAggregationCountFirstUnits);
            }
        }
        else
        {
            if (alive_bins != nullptr)
                pruned_records += merge_unit(AliveBinRecords{alive_bins});
            else
                pruned_records += merge_unit(AllStagedRecords{});
            unit_groups = table.size();
        }

        if (full_group_count)
            *full_group_count += unit_groups;
        if (updater)
            updater->recordAggregationStateSizes(dest, bucket);

        /// Filled by a conversion that materializes only some of the unit's groups - the top-K one or the HAVING
        /// pre-filter - when the statistics ask for it: the untruncated key bytes, because the chunk carries only the
        /// kept groups, and a bounded sample of those keys for when it carries none at all.
        UntruncatedAggregationKeys untruncated_keys;
        auto chunk = convertOneBucketToChunk(dest, arena, final, bucket, updater ? &untruncated_keys : nullptr, /*keep_table_buffer=*/true);
        if (updater)
        {
            if (untruncated_keys.bytes)
                updater->recordAggregationKeySizes(
                    chunk.chunk, keys_positions, key_types, untruncated_keys.bytes, untruncated_keys.sample_columns);
            else
                updater->recordAggregationKeySizes(chunk.chunk, keys_positions, key_types);
        }
        if (pruning)
            pruning->offerCounts(
                assert_cast<const ColumnUInt64 &>(*chunk.chunk.getColumns()[params.keys_size + params.bucket_top_k_rank_index]).getData());
        chunks.push_back(std::move(chunk));
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMergeUnits);

        /// The conversion copied the keys out of the records, so the unit's records go now.
        for (size_t sub = unit_first_sub; sub < unit_end_sub; ++sub)
            staged.release(sub);

        /// The next unit creates its states in a fresh arena, so the states of this one free with its arena: at once
        /// where the conversion copied their results out, or with the chunk where they are part of it, as the non-final
        /// and -State results are, whose columns captured the arena.
        if (unit + 1 < units)
        {
            arena_slot = std::make_shared<Arena>();
            arena = arena_slot.get();
        }
    }

    if (pruned_records)
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPrunedRecords, pruned_records);

    /// Every source cell of the bucket was adopted, merged or discarded and its mapped value nulled, so the
    /// sources' bucket tables only release their buffers.
    for (size_t i = 1; i < data.size(); ++i)
        getDataVariant<Method>(*data[i]).data.impls[bucket].clearAndShrink();

    /// Both tables are empty after conversion, so the bucket retains the counting buffer for the next bucket.
    if (count_first)
    {
        chassert(table.empty() && counting_table.empty());
        std::swap(table, counting_table);
    }

    /// Every unit converted successfully: the output chunks either copied the values out of the arenas of their units or
    /// captured their ownership, so the slot, which holds the arena of the last unit, retires now. A throw or a
    /// cancellation returns before this and leaves every slot that is not retired to the ordinary destruction of the
    /// destination.
    arena_slot.reset();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
    return chunks;
}

AggregatedDataVariantsPtr Aggregator::createAdaptiveMergeDestination() const
{
    auto destination = std::make_shared<AggregatedDataVariants>();
    destination->aggregator = this;
    destination->keys_size = params.keys_size;
    destination->key_sizes = key_sizes;
    destination->init(convertToTwoLevelTypeIfPossible(method_chosen));
    /// The drain and the merge create the destination's states in per-unit arenas, which the slot of each bucket holds
    /// one at a time, for two reasons. Fresh arenas, rather than a source table's, because with a zero-size aggregate
    /// state (`Nothing`) an arena returns one address for every allocation, so a drained state would alias that table's
    /// states and the bucket merge would see a state merged into itself. And per unit, rather than per source, so a
    /// converted unit's states free with its arena (see `mergeAndConvertAdaptiveBucket`). The slots live outside
    /// `aggregates_pools`, which every bucket's output columns capture wholesale; each conversion is handed its own slot
    /// instead.
    destination->adaptive_merge_bucket_arenas.resize(ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    for (auto & slot : destination->adaptive_merge_bucket_arenas)
        slot = std::make_shared<Arena>();
    return destination;
}

void Aggregator::mergeAdaptiveSourceStates(
    PaddedPODArray<AggregateDataPtr> & places,
    const PaddedPODArray<AggregateDataPtr *> & source_slots,
    AdaptiveMergeTask & task,
    const AdaptiveAggregationSession & session,
    Arena * arena,
    std::atomic<bool> & is_cancelled) const
{
    const size_t merges = places.size();
    chassert(merges == source_slots.size());
    chassert(params.max_threads > 0 && session.finished_producers > 0);

    /// A group larger than a worker's share can hold up the merge. Very large states amortize the pool even
    /// when other groups are busy, while small states do not justify its setup even with idle workers.
    const size_t min_parallel_work = std::clamp(
        session.estimated_merge_work / std::min(params.max_threads, session.finished_producers),
        adaptive_parallel_merge_min_work,
        adaptive_parallel_merge_max_work);

    auto & order = task.merge_order;
    auto & group = task.merge_group;
    bool ordered = false;
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const IAggregateFunction & function = *aggregate_functions[i];
        const size_t offset = offsets_of_aggregate_states[i];
        if (!function.isAbleToParallelizeMerge() || !function.isParallelizeMergePrepareNeeded())
        {
            group.resize(merges);
            for (size_t j = 0; j < merges; ++j)
                group[j] = *source_slots[j] + offset;
            function.mergeBatch(0, merges, places.data(), offset, group.data(), *thread_pool, is_cancelled, arena);
            continue;
        }

        /// The merges of one destination are contiguous in this order. A giant set of the merge is the state of a group
        /// most producers hold, merged pairwise in one task: the tail of the whole merge. Merged together, the sets are
        /// converted to two-level in parallel where they are large (`parallelizeMergePrepare`), and each of their
        /// buckets is merged on the pool (`parallelizeMergeMulti`).
        if (!ordered)
        {
            order.resize(merges);
            std::iota(order.begin(), order.end(), 0);
            std::ranges::stable_sort(order, [&](UInt32 lhs, UInt32 rhs) { return std::less<AggregateDataPtr>{}(places[lhs], places[rhs]); });
            ordered = true;
        }
        for (size_t begin = 0; begin < merges;)
        {
            size_t end = begin + 1;
            while (end < merges && places[order[end]] == places[order[begin]])
                ++end;
            size_t group_work = function.getEstimatedMergeWork(places[order[begin]] + offset);
            for (size_t k = begin; k < end; ++k)
                group_work += function.getEstimatedMergeWork(*source_slots[order[k]] + offset);

            if (group_work <= min_parallel_work)
            {
                for (size_t k = begin; k < end; ++k)
                    function.merge(places[order[k]] + offset, *source_slots[order[k]] + offset, arena);
            }
            else
            {
                group.clear();
                group.push_back(places[order[begin]] + offset);
                for (size_t k = begin; k < end; ++k)
                    group.push_back(*source_slots[order[k]] + offset);
                function.parallelizeMergePrepare(group, *thread_pool, is_cancelled);
                function.parallelizeMergeMulti(group, *thread_pool, is_cancelled, arena);
            }
            begin = end;
        }
    }

    /// Source tables retain ownership until every aggregate has merged successfully, so an exception
    /// leaves complete states for their ordinary cleanup. Then the sources are destroyed, which does not throw,
    /// and their slots cleared.
    auto & sources = task.merge_group;
    sources.resize(merges);
    for (size_t j = 0; j < merges; ++j)
        sources[j] = *source_slots[j];
    for (size_t i = 0; i < params.aggregates_size; ++i)
        if (!aggregate_functions[i]->hasTrivialDestructor())
            aggregate_functions[i]->destroyBatch(0, merges, sources.data(), offsets_of_aggregate_states[i]);
    for (AggregateDataPtr * slot : source_slots)
        *slot = nullptr;
}

}
