/// The merge kernels of the adaptive aggregation: the partition-wise merge of the staged records with the source
/// tables, and the conversion of each bucket. They are member templates of `Aggregator` (defined here rather than in
/// `Aggregator.cpp`, following `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <bit>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CacheLine.h>
#include <Common/ProfileEvents.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/AdaptiveAggregation/AdaptiveAggregationImpl.h>
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

    /// Emplaces a staged key, read back by `readStagedKey`, into `table` with its routing hash.
    template <typename Key, typename Table>
    void ALWAYS_INLINE emplaceStagedKey(
        Table & table, const char * key_pos, size_t key_size, size_t routing_hash, typename Table::LookupResult & it, bool & inserted)
    {
        table.emplace(readStagedKey<Key>(key_pos, key_size, routing_hash), it, inserted, routing_hash);
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

    /// Calls `callback(record, bytes, hash)` for every general record of `ranges`, in the record shape the producers
    /// chose for `Key` and the argument layout (see `Aggregator::appendDelayedRecords`), with the routing hash of its
    /// key as `table` hashes it.
    template <typename Key, typename Table, typename Callback>
    void forEachArgumentRecord(
        const Table & table, const DB::AdaptiveArgumentLayout & argument_layout, const DB::AdaptiveRecordRanges & ranges, Callback && callback)
    {
        bool fixed_stride = false;
        if constexpr (!adaptive_key_stages_bytes<Key>)
            fixed_stride = argument_layout.variable_fields.empty();

        if (fixed_stride)
        {
            using Record = StagedFixedArgumentRecord<Key>;
            const size_t bytes = Record::bytes(argument_layout.fixed_bytes);
            for (const auto & range : ranges)
                for (const char * record = range.data(); record < range.data() + range.size(); record += bytes)
                    callback(record, bytes, Record::hash(table, record));
        }
        else
        {
            for (const auto & range : ranges)
            {
                for (const char * record = range.data(); record < range.data() + range.size();)
                {
                    const size_t bytes = StagedArgumentRecord::bytes(record);
                    callback(record, bytes, StagedArgumentRecord::hash(record));
                    record += bytes;
                }
            }
        }
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

    /// A partition block read back from a bucket's spill stream (see `AdaptiveAggregationSession::spilled_buckets`):
    /// the partition within the bucket, its record count and its records, in a padded buffer like a chunk's.
    struct SpilledPartitionRecords
    {
        size_t sub = 0;
        size_t records = 0;
        DB::PaddedPODArray<char> data;
    };

    /// Reads the bucket's spill stream back and removes it. Called by the one task that merges the bucket, after the
    /// finish barrier, so no producer writes to the stream any more.
    std::vector<SpilledPartitionRecords> readSpilledBucket(DB::AdaptiveAggregationSession & session, size_t bucket)
    {
        std::vector<SpilledPartitionRecords> blocks;
        auto & spilled = session.spilled_buckets[bucket];
        if (!spilled.stream)
            return blocks;

        spilled.stream->finishWriting();
        auto in = spilled.stream->read();
        while (!in->eof())
        {
            UInt32 sub = 0;
            UInt64 records = 0;
            UInt64 bytes = 0;
            DB::readBinaryLittleEndian(sub, *in);
            DB::readBinaryLittleEndian(records, *in);
            DB::readBinaryLittleEndian(bytes, *in);
            auto & block = blocks.emplace_back();
            block.sub = sub;
            block.records = records;
            block.data.resize(bytes);
            in->readStrict(block.data.data(), bytes);
        }
        spilled.stream.reset();
        return blocks;
    }

    /// The records of one partition: its chunks of every producer, then its blocks read back from the spill stream.
    void collectPartitionRecords(
        const DB::AdaptiveAggregationSession & session,
        const std::vector<SpilledPartitionRecords> & spilled,
        size_t partition,
        size_t sub,
        DB::AdaptiveRecordRanges & ranges)
    {
        ranges.clear();
        for (const auto & producer : session.producer_buffers)
            producer->forEachChunk(partition, [&](std::string_view records) { ranges.push_back(records); });
        for (const auto & block : spilled)
            if (block.sub == sub)
                ranges.emplace_back(block.data.data(), block.data.size());
    }

    /// The `limit` groups with the largest counts, kept up to date while the counts grow, so that no pass over all the
    /// counted groups selects them afterwards. Counts only grow, so the smallest count of a full top only rises: an
    /// update whose count does not exceed it cannot change the top and costs one comparison. A growing count leaves
    /// the smallest counts, so an ascending order cannot be kept this way.
    ///
    /// The caller keeps the count of every group in a 64-bit word and passes the word to `update` after each growth.
    /// The low `count_bits` bits hold the count. The high bits belong to the top and are zero in the word of a new
    /// group: there the top keeps the index of the group's entry plus one, so it finds the entry of an updated group
    /// without a search. A group evicted from the top keeps its stale index, so the entry an index names is checked by
    /// the full key: a hash does not identify a group. Keys are kept as given, so memory that a key views must outlive
    /// the updates.
    template <typename Key>
    class GrowingCountTopK
    {
    public:
        /// A group of the top: its key, its hash, which the top only keeps for the caller, and its count.
        struct Group
        {
            Key key;
            UInt64 hash;
            UInt64 count;
        };

        /// The index bits hold every index of the largest limit plus one, and the count bits more rows than an
        /// aggregation reads.
        static constexpr size_t count_bits = 64 - std::bit_width(DB::Aggregator::Params::max_bucket_top_k);
        static constexpr UInt64 count_mask = (UInt64{1} << count_bits) - 1;

        explicit GrowingCountTopK(size_t limit_)
            : limit(limit_)
        {
            chassert(limit > 0 && limit <= DB::Aggregator::Params::max_bucket_top_k);
        }

        /// Starts over with no groups. The words of the groups passed before must not be passed again: their indices
        /// name entries that are gone.
        void clear()
        {
            groups.clear();
            heap.clear();
            heap_positions.clear();
            min_count = 0;
        }

        /// Takes the word of a group whose count grew.
        void ALWAYS_INLINE update(UInt64 & word, const Key & key, UInt64 hash)
        {
            const UInt64 count = word & count_mask;
            /// Until the top is full, `min_count` is zero, so every count enters, being positive.
            chassert(count > 0);
            if (count > min_count)
                enter(word, count, key, hash);
        }

        /// The groups with the `limit` largest counts so far, or all of them while there are fewer, in no particular
        /// order. Which of the groups with equal counts at the smallest are kept is unspecified.
        const std::vector<Group> & getGroups() const { return groups; }

    private:
        NO_INLINE void enter(UInt64 & word, UInt64 count, const Key & key, UInt64 hash)
        {
            /// The high bits of the word name the group's entry by its index plus one, or no entry by zero.
            const UInt64 entry = word >> count_bits;
            chassert(entry <= groups.size());
            if (entry && groups[entry - 1].key == key)
            {
                /// A grown count can only move its index away from the root of the min-heap.
                groups[entry - 1].count = count;
                siftDown(heap_positions[entry - 1]);
            }
            else
            {
                UInt32 index = 0;
                if (heap.size() < limit)
                {
                    index = static_cast<UInt32>(groups.size());
                    groups.push_back({key, hash, count});
                    heap.push_back(index);
                    heap_positions.push_back(static_cast<UInt32>(heap.size() - 1));
                    siftUp(heap.size() - 1);
                }
                else
                {
                    /// The group with the smallest count leaves the top, and the new one takes its entry.
                    index = heap.front();
                    groups[index] = {key, hash, count};
                    siftDown(0);
                }
                word = count | (UInt64{index + 1} << count_bits);
            }
            if (heap.size() == limit)
                min_count = groups[heap.front()].count;
        }

        /// Moves the index at `position` of the min-heap toward the root while its count is below its parent's.
        void siftUp(size_t position)
        {
            const UInt32 index = heap[position];
            while (position > 0)
            {
                const size_t parent = (position - 1) / 2;
                if (groups[heap[parent]].count <= groups[index].count)
                    break;
                place(position, heap[parent]);
                position = parent;
            }
            place(position, index);
        }

        /// Moves the index at `position` of the min-heap away from the root while its count is above a child's.
        void siftDown(size_t position)
        {
            const UInt32 index = heap[position];
            while (true)
            {
                size_t child = 2 * position + 1;
                if (child >= heap.size())
                    break;
                if (child + 1 < heap.size() && groups[heap[child + 1]].count < groups[heap[child]].count)
                    ++child;
                if (groups[index].count <= groups[heap[child]].count)
                    break;
                place(position, heap[child]);
                position = child;
            }
            place(position, index);
        }

        /// Puts `index` at `position` of the min-heap.
        void place(size_t position, UInt32 index)
        {
            heap[position] = index;
            heap_positions[index] = static_cast<UInt32>(position);
        }

        const size_t limit;
        /// The groups by their indices, the indices as a min-heap by count, and the position of every index in it.
        std::vector<Group> groups;
        std::vector<UInt32> heap;
        std::vector<UInt32> heap_positions;
        /// The smallest count of the top once it is full, zero before.
        UInt64 min_count = 0;
    };

    /// The same for a set table, whose cells hold only keys.
    template <typename Table, typename Callback>
    void forEachKeyCellWithHash(Table & table, Callback && callback)
    {
        for (auto it = table.begin(), end = table.end(); it != end; ++it)
            callback(Table::cell_type::getKey(it->getValue()), it.getHash());
    }

}

template <typename Method, typename Table, typename OnCount>
size_t NO_INLINE Aggregator::drainAdaptivePartition(
    Table & table,
    Arena * arena,
    const AdaptiveRecordRanges & ranges,
    const bool * alive_bins,
    PaddedPODArray<AggregateDataPtr> & places,
    RowStorePointers & records,
    [[maybe_unused]] const OnCount & on_count) const
{
    using Key = AdaptiveRecordKey<Method>;
    constexpr bool count_only = !std::is_same_v<OnCount, std::nullptr_t>;

    /// Walks the partition's records in order. The table slot of the record
    /// `adaptive_drain_prefetch_look_ahead` positions ahead is prefetched only into a table that
    /// has outgrown the cache, or for string keys, whose emplace compares key bytes behind the
    /// slot; prefetching a cache-resident slot is pure overhead.
    const bool prefetch
        = adaptive_key_stages_bytes<Key> || table.getBufferSizeInBytes() > adaptive_drain_prefetch_min_table_bytes;
    size_t drained = 0;
    size_t skipped = 0;
    /// The callers' lambdas run once per record, so all of them are inlined into the walk. `apply` receives the record
    /// with the routing hash of its key, which `hash_of` reads from the record or recomputes from its key.
    const auto walk = [&](auto record_bytes, auto key_of, auto hash_of, auto apply) ALWAYS_INLINE
    {
        const auto walk_ranges = [&]<bool with_prefetch, bool filtered>() ALWAYS_INLINE
        {
            /// Whether the walk takes a record of the hash: every one, or with alive bins only those of an alive bin.
            const auto takes = [&]([[maybe_unused]] UInt64 hash) ALWAYS_INLINE
            {
                if constexpr (filtered)
                    return alive_bins[bucketCountBin(hash)];
                else
                    return true;
            };

            /// The prefetch cursor runs the look-ahead distance in front of the walk and crosses from one range to
            /// the next as the walk does: a partition's ranges are as small as a first chunk of a few kilobytes, so
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
                    /// A range is cold until the cursor reads its records, and the hardware prefetchers take up a
                    /// new stream only after its first misses: the following range, up to a page, is requested into
                    /// L2 as the cursor enters this one.
                    if (ahead_range < ranges.size())
                    {
                        const char * next = ranges[ahead_range].data();
                        const char * const next_end = next + std::min<size_t>(ranges[ahead_range].size(), adaptive_drain_range_prefetch_bytes);
                        for (; next < next_end; next += CH_CACHE_LINE_SIZE)
                            __builtin_prefetch(next, /*rw=*/0, /*locality=*/2);
                    }
                }
                const UInt64 hash = hash_of(ahead);
                if (takes(hash))
                {
                    const auto [key_pos, key_size] = key_of(ahead);
                    prefetchStagedKey<Key>(table, key_pos, key_size, hash);
                }
                ahead += record_bytes(ahead);
            };
            if constexpr (with_prefetch)
                for (size_t i = 0; i < adaptive_drain_prefetch_look_ahead; ++i)
                    prefetch_next();

            for (const auto & range : ranges)
            {
                const char * record = range.data();
                const char * const end = range.data() + range.size();
                while (record < end)
                {
                    if constexpr (with_prefetch)
                        prefetch_next();
                    const UInt64 hash = hash_of(record);
                    if (takes(hash))
                    {
                        apply(record, hash);
                        ++drained;
                    }
                    else
                    {
                        ++skipped;
                    }
                    record += record_bytes(record);
                }
            }
        };
        const auto walk_filtered_or_not = [&]<bool with_prefetch>() ALWAYS_INLINE
        {
            if (alive_bins != nullptr)
                walk_ranges.template operator()<with_prefetch, true>();
            else
                walk_ranges.template operator()<with_prefetch, false>();
        };
        if (prefetch)
            walk_filtered_or_not.template operator()<true>();
        else
            walk_filtered_or_not.template operator()<false>();
    };

    if (is_simple_count)
    {
        using Record = StagedKeyRecord<Key, true>;
        walk(
            [](const char * record) ALWAYS_INLINE { return Record::bytes(Record::keySize(record)); },
            [](const char * record) ALWAYS_INLINE { return std::pair{record + Record::key_offset, Record::keySize(record)}; },
            [&](const char * record) ALWAYS_INLINE { return Record::hash(table, record); },
            [&](const char * record, UInt64 hash) ALWAYS_INLINE
            {
                typename Table::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<Key>(table, record + Record::key_offset, Record::keySize(record), hash, it, inserted);
                if constexpr (MapAggregationMethod<Method>)
                {
                    if (inserted)
                        getInlineCountState(it->getMapped()) = Record::count(record);
                    else
                        getInlineCountState(it->getMapped()) += Record::count(record);
                }
            });
    }
    else if (!params.aggregates_size)
    {
        /// A map method without aggregate functions still gives a new key a place, of no size.
        using Record = StagedKeyRecord<Key, false>;
        walk(
            [](const char * record) ALWAYS_INLINE { return Record::bytes(Record::keySize(record)); },
            [](const char * record) ALWAYS_INLINE { return std::pair{record + Record::key_offset, Record::keySize(record)}; },
            [&](const char * record) ALWAYS_INLINE { return Record::hash(table, record); },
            [&](const char * record, UInt64 hash) ALWAYS_INLINE
            {
                typename Table::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<Key>(table, record + Record::key_offset, Record::keySize(record), hash, it, inserted);
                if constexpr (MapAggregationMethod<Method>)
                {
                    if (inserted)
                    {
                        it->getMapped() = nullptr;
                        AggregateDataPtr place = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                        createAggregateStates(place);
                        it->getMapped() = place;
                    }
                }
            });
    }
    else if constexpr (MapAggregationMethod<Method>)
    {
        const auto & argument_layout = *adaptive_argument_layout;

        /// The same record shapes the producers chose (see `appendDelayedRecords`).
        bool fixed_stride = false;
        if constexpr (!adaptive_key_stages_bytes<Key>)
            fixed_stride = argument_layout.variable_fields.empty();

        const auto key_of = [&](const char * record) ALWAYS_INLINE
        {
            return std::pair{
                record + StagedArgumentRecord::keyOffset(argument_layout.fixed_bytes), StagedArgumentRecord::keySize(record)};
        };

        if constexpr (count_only)
        {
            const auto count = [&](const char * key_pos, size_t key_size, UInt64 hash) ALWAYS_INLINE
            {
                typename Table::LookupResult it;
                bool inserted = false;
                const Key key = readStagedKey<Key>(key_pos, key_size, hash);
                table.emplace(key, it, inserted, hash);
                UInt64 & group_count = getInlineCountState(it->getMapped());
                if (inserted)
                    group_count = 1;
                else
                    ++group_count;
                on_count(group_count, key, hash);
            };
            if (fixed_stride)
            {
                using Record = StagedFixedArgumentRecord<Key>;
                const size_t bytes = Record::bytes(argument_layout.fixed_bytes);
                walk(
                    [bytes](const char *) ALWAYS_INLINE { return bytes; },
                    [](const char * record) ALWAYS_INLINE { return std::pair{record + Record::key_offset, sizeof(Key)}; },
                    [&](const char * record) ALWAYS_INLINE { return Record::hash(table, record); },
                    [&](const char * record, UInt64 hash) ALWAYS_INLINE { count(record + Record::key_offset, sizeof(Key), hash); });
            }
            else
            {
                walk(
                    [](const char * record) ALWAYS_INLINE { return StagedArgumentRecord::bytes(record); },
                    key_of,
                    [](const char * record) ALWAYS_INLINE { return StagedArgumentRecord::hash(record); },
                    [&](const char * record, UInt64 hash) ALWAYS_INLINE
                    {
                        const auto [key_pos, key_size] = key_of(record);
                        count(key_pos, key_size, hash);
                    });
            }
            /// The records were counted, not drained into states.
            return skipped;
        }

        bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
        use_compiled_functions = compiled_aggregate_functions_holder != nullptr;
#endif

        places.clear();
        records.ptrs.clear();
        const auto apply = [&](const char * record, const char * key_pos, size_t key_size, UInt64 hash) ALWAYS_INLINE
        {
            typename Table::LookupResult it;
            bool inserted = false;
            emplaceStagedKey<Key>(table, key_pos, key_size, hash, it, inserted);
            if (inserted)
            {
                it->getMapped() = nullptr;
                AggregateDataPtr place = arena->alignedAlloc(total_size_of_aggregate_states, align_aggregate_states);
                createAggregateStates(place, use_compiled_functions);
                it->getMapped() = place;
            }
            places.push_back(it->getMapped());
            records.ptrs.push_back(record);
        };

        size_t fixed_arguments_offset = StagedArgumentRecord::header_bytes;
        if (fixed_stride)
        {
            using Record = StagedFixedArgumentRecord<Key>;
            const size_t bytes = Record::bytes(argument_layout.fixed_bytes);
            const auto fixed_key_of = [](const char * record) ALWAYS_INLINE { return std::pair{record + Record::key_offset, sizeof(Key)}; };
            walk(
                [bytes](const char *) ALWAYS_INLINE { return bytes; },
                fixed_key_of,
                [&](const char * record) ALWAYS_INLINE { return Record::hash(table, record); },
                [&](const char * record, UInt64 hash) ALWAYS_INLINE { apply(record, record + Record::key_offset, sizeof(Key), hash); });
            fixed_arguments_offset = Record::arguments_offset;
        }
        else
        {
            walk(
                [](const char * record) ALWAYS_INLINE { return StagedArgumentRecord::bytes(record); },
                key_of,
                [](const char * record) ALWAYS_INLINE { return StagedArgumentRecord::hash(record); },
                [&](const char * record, UInt64 hash) ALWAYS_INLINE
                {
                    const auto [key_pos, key_size] = key_of(record);
                    apply(record, key_pos, key_size, hash);
                });
        }

        const size_t rows = places.size();
        if (rows)
        {
            /// The arguments are rebuilt into dense columns while the records are hot, so the
            /// ordinary batch executor, and the compiled functions, apply to them as to a block.
            Columns arguments(argument_layout.num_positions);
            if (const auto & field = argument_layout.key_field)
            {
                const size_t key_offset = fixed_stride
                    ? StagedFixedArgumentRecord<Key>::key_offset : StagedArgumentRecord::keyOffset(argument_layout.fixed_bytes);
                auto column = field->type->createColumn();
                const size_t key_size = column->sizeOfValueIfFixed();
                chassert(key_size == sizeof(Key));
                column->fillFromRowStorePtrs(field->type, records, key_offset, key_size, 0, rows);
                arguments[field->position] = std::move(column);
            }
            for (const auto & field : argument_layout.fixed_fields)
            {
                auto column = field.type->createColumn();
                column->fillFromRowStorePtrs(field.type, records, fixed_arguments_offset + field.offset, field.size, 0, rows);
                arguments[field.position] = std::move(column);
            }

            if (!argument_layout.variable_fields.empty())
            {
                MutableColumns variable_columns;
                variable_columns.reserve(argument_layout.variable_fields.size());
                for (const auto & field : argument_layout.variable_fields)
                {
                    variable_columns.push_back(field.type->createColumn());
                    variable_columns.back()->reserve(rows);
                }

                const IColumn::SerializationSettings serialization_settings;
                for (const char * record : records.ptrs)
                {
                    const auto [key_pos, key_size] = key_of(record);
                    const char * values = key_pos + key_size;
                    ReadBufferFromMemory in(values, record + StagedArgumentRecord::bytes(record) - values);
                    for (auto & column : variable_columns)
                        column->deserializeAndInsertFromArena(in, &serialization_settings);
                }

                for (size_t j = 0; j < variable_columns.size(); ++j)
                    arguments[argument_layout.variable_fields[j].position] = std::move(variable_columns[j]);
            }

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
    }

    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationDrainedRecords, drained);
    return skipped;
}

size_t Aggregator::collectAdaptiveTableStatistics(AggregatedDataVariants & variants, UInt16 * bins) const
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
/// NOLINTNEXTLINE(readability-non-const-parameter): The map-table callback updates the count bins.
size_t Aggregator::collectAdaptiveTableStatistics(Method & method, Arena * arena, UInt16 * bins) const
{
    size_t work = 0;
    if constexpr (MapAggregationMethod<Method>)
    {
        /// A `uniqExact` or `uniqExactIf` rank adds the distinct count of the cell: the distinct count of a merged group
        /// is at most the sum of those of the cells and records merged into it, so the bins bound it as they bound a row
        /// count.
        ColumnUInt64::MutablePtr scratch;
        if (bins && !is_simple_count && !bucket_top_k_ranks_by_count_state)
            scratch = ColumnUInt64::create();
        const auto rank_count = [&](AggregateDataPtr & mapped) -> UInt64
        {
            if (is_simple_count)
                return getInlineCountState(mapped);
            if (bucket_top_k_ranks_by_count_state)
                return getCountState(mapped + offsets_of_aggregate_states[params.bucket_top_k_rank_index]);
            return finalizeBucketTopKRank(mapped, *scratch, arena);
        };
        const auto add = [&](auto & table)
        {
            forEachMappedCellWithHashOnDemand(
                table,
                [&](AggregateDataPtr & mapped, const auto & hash_of)
                {
                    if (bins)
                        addToCountBin(bins[adaptiveCountBin(hash_of())], rank_count(mapped));
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

Aggregator::AggregatedChunks Aggregator::mergeAndConvertAdaptiveBucket(
    ManyAggregatedDataVariants & data,
    AdaptiveAggregationSession & session,
    AdaptiveMergeScratch & scratch,
    bool final,
    Int32 bucket,
    Int32 previous_bucket,
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
                dest, method, data, session, scratch, final, bucket, previous_bucket, is_cancelled, updater, full_group_count);
        });
    return chunks;
}

template <typename Method>
Aggregator::AggregatedChunks Aggregator::mergeAndConvertAdaptiveBucketImpl(
    AggregatedDataVariants & dest,
    Method & dest_method,
    ManyAggregatedDataVariants & data,
    AdaptiveAggregationSession & session,
    AdaptiveMergeScratch & scratch,
    bool final,
    Int32 bucket,
    Int32 previous_bucket,
    std::atomic<bool> & is_cancelled,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater,
    size_t * full_group_count) const
{
    /// The task's table, emptied by the conversion of its previous bucket's last unit, moves into this bucket's slot:
    /// the slots are exclusively owned by the tasks that merge their buckets, and a retired bucket's slot is never
    /// read again. Insertion grows the table by its distinct keys, and conversion retains that capacity
    /// for the next unit or bucket. Reserving by the record count would oversize tables with repeated keys,
    /// increasing the cost of probing, scanning and clearing them.
    if (previous_bucket >= 0)
        std::swap(dest_method.data.impls[bucket], dest_method.data.impls[previous_bucket]);

    using Table = std::decay_t<decltype(dest_method.data.impls[0])>;
    static_assert(std::decay_t<decltype(dest_method.data)>::NUM_BUCKETS == ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    auto & table = dest_method.data.impls[bucket];
    Arena * arena = dest.adaptive_merge_bucket_arenas[bucket].get();

    const AdaptivePartitionLayout layout = session.layout;
    const size_t partitions_per_bucket = layout.partitionsPerBucket();
    const size_t first_partition = layout.firstPartition(bucket);

    /// The spilled records of the bucket stay in memory until the bucket is done: the units' tables point into them.
    const auto spilled = readSpilledBucket(session, bucket);

    std::vector<size_t> partition_records(partitions_per_bucket);
    for (const auto & producer : session.producer_buffers)
        for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
            partition_records[sub] += producer->recordsOf(first_partition + sub);
    for (const auto & block : spilled)
        partition_records[block.sub] += block.records;
    size_t bucket_records = 0;
    for (const size_t records : partition_records)
        bucket_records += records;

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

    /// With the top-K pruning, the bounds of the bucket's count bins (see `AdaptiveTopKPruning`). The statistics of the
    /// updater need every group converted, so a merge that collects them does not prune.
    AdaptiveTopKPruning * const pruning = updater ? nullptr : session.top_k_pruning.get();
    std::array<UInt64, adaptive_count_bins_per_bucket> bin_bounds{};
    if (pruning)
        bin_bounds = sumBucketCountBins(*pruning, bucket);
    std::array<bool, adaptive_count_bins_per_bucket> alive{};
    const size_t bins_per_unit = adaptive_count_bins_per_bucket / units;
    size_t pruned_records = 0;

    /// A top-K by a count among other aggregates keeps only each unit's best groups by the count in the final
    /// conversion, so the other aggregate states of the rest of the groups would be built for nothing: a count-first
    /// unit counts its groups first and builds the states of its best groups only (see below). A merge that collects
    /// the statistics needs every group converted.
    const bool count_first = MapAggregationMethod<Method> && final && params.bucket_top_k && bucket_top_k_ranks_by_count_state
        && !is_simple_count && !updater;
    const size_t rank_offset = count_first ? offsets_of_aggregate_states[params.bucket_top_k_rank_index] : 0;
    /// With a descending order, a count-first unit keeps its best groups up to date while it counts them. An ascending
    /// order cannot be kept this way, so a pass over the counted groups selects them.
    std::optional<GrowingCountTopK<typename Table::key_type>> top_k;
    if (count_first && !params.bucket_top_k_ascending)
        top_k.emplace(params.bucket_top_k);
    /// A count-first unit keeps integer counts outside `AggregatedDataVariants`, whose cleanup treats mapped values
    /// as aggregate-state pointers. Its counting table takes the buffer retained from the previous bucket, while the
    /// bucket's own table holds the states of the best groups and remains valid during exception unwinding.
    Table counting_table;
    if (count_first)
        std::swap(table, counting_table);

    /// A source cell whose group cannot reach the top goes with its states.
    const auto discard_cell = [&](SourceCell & cell)
    {
        if constexpr (MapAggregationMethod<Method>)
        {
            AggregateDataPtr & place = *cell.mapped;
            if (!is_simple_count && !all_aggregates_has_trivial_destructor)
                for (size_t i = 0; i < params.aggregates_size; ++i)
                    aggregate_functions[i]->destroy(place + offsets_of_aggregate_states[i]);
            place = nullptr;
        }
    };

    AggregatedChunks chunks;
    auto & places = scratch.places;
    auto & source_places = scratch.source_places;
    for (size_t unit = 0; unit < units; ++unit)
    {
        if (is_cancelled.load(std::memory_order_seq_cst))
            return chunks;

        const size_t unit_first_partition = first_partition + unit * partitions_per_unit;
        size_t unit_records = 0;
        for (size_t sub = unit * partitions_per_unit; sub < (unit + 1) * partitions_per_unit; ++sub)
            unit_records += partition_records[sub];
        auto & cells = unit_cells[unit];
        if (!unit_records && cells.empty())
            continue;

        /// The unit's bins that can still hold a group of the top, at the threshold of the moment: a unit with none is
        /// skipped, its source cells dropped and its records freed unread; a unit with some drains only theirs.
        const bool * alive_bins = nullptr;
        size_t alive_count = bins_per_unit;
        if (pruning)
        {
            const UInt64 threshold = pruning->threshold.load(std::memory_order_relaxed);
            alive_count = 0;
            for (size_t bin = unit * bins_per_unit; bin < (unit + 1) * bins_per_unit; ++bin)
            {
                alive[bin] = bin_bounds[bin] >= threshold;
                alive_count += alive[bin];
            }
            if (!alive_count)
            {
                for (auto & cell : cells)
                    discard_cell(cell);
                for (const auto & producer : session.producer_buffers)
                    for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                        producer->releasePartition(partition);
                pruned_records += unit_records;
                ProfileEvents::increment(ProfileEvents::AdaptiveAggregationPrunedUnits);
                continue;
            }
            if (alive_count < bins_per_unit)
                alive_bins = alive.data();
        }

        /// The groups of the unit, which a count-first unit does not keep in its table.
        size_t unit_groups = 0;
        const auto merge_source_states = [&]
        {
            if (source_places.empty())
                return;
            mergeAdaptiveSourceStates(scratch, session, arena, is_cancelled);
            for (auto & cell : cells)
                *cell.mapped = nullptr;
        };
        if (count_first)
        {
            if constexpr (MapAggregationMethod<Method>)
            {
                using Key = AdaptiveRecordKey<Method>;

                /// The first pass counts the groups in the table, each count kept in the mapped value as a lone
                /// `count()` keeps it: a source cell adds its count state, a record one row. With a descending order, the
                /// top takes every grown count and keeps its indices in the high bits of the counts.
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
                        top_k->update(group_count, cell.key, cell.hash);
                }
                for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                {
                    collectPartitionRecords(session, spilled, partition, layout.subOf(partition), scratch.ranges);
                    pruned_records += drainAdaptivePartition<Method>(
                        counting_table,
                        arena,
                        scratch.ranges,
                        alive_bins,
                        places,
                        scratch.records,
                        [&](UInt64 & group_count, const Key & key, UInt64 hash) ALWAYS_INLINE
                        {
                            if (top_k)
                                top_k->update(group_count, key, hash);
                        });
                }
                unit_groups = counting_table.size();

                /// The unit's best groups by their counts, which are exact: the unit holds every record and source cell
                /// of its keys.
                auto & best_hashes = scratch.best_hashes;
                best_hashes.clear();
                if (top_k)
                {
                    /// With the pruning, which serves a descending order only, a group counted below the threshold
                    /// cannot reach the top.
                    const UInt64 threshold = pruning ? pruning->threshold.load(std::memory_order_relaxed) : 0;
                    for (const auto & group : top_k->getGroups())
                        if (group.count >= threshold)
                            best_hashes.push_back(group.hash);
                }
                else
                {
                    /// The smallest counts, selected by a pass over the counted groups. Only the cells that enter the heap
                    /// are hashed; its front holds the largest count kept.
                    auto & best = scratch.best_counts_and_hashes;
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
                std::sort(best_hashes.begin(), best_hashes.end());
                /// By hash: a group that shares the hash of a best one is merged completely as well, and the conversion
                /// ranks it by its count with the others. The second pass tests every record, so a bitset on 12 hash bits
                /// answers for nearly all of them with one predictable branch, before the search that would mispredict
                /// on random hashes.
                std::array<UInt64, 64> best_filter{};
                for (const UInt64 hash : best_hashes)
                    best_filter[(hash >> 6) & 63] |= UInt64{1} << (hash & 63);
                const auto is_best = [&](UInt64 hash)
                {
                    return ((best_filter[(hash >> 6) & 63] >> (hash & 63)) & 1)
                        && std::binary_search(best_hashes.begin(), best_hashes.end(), hash);
                };

                /// The second pass builds the states of the best groups in the bucket's table for the conversion:
                /// their source cells are adopted or merged and their records drained as in the ordinary
                /// merge, and the other source cells go with their states.
                counting_table.clear();
                places.clear();
                source_places.clear();
                for (auto & cell : cells)
                {
                    if (!is_best(cell.hash))
                    {
                        discard_cell(cell);
                        continue;
                    }
                    typename Table::LookupResult it;
                    bool inserted = false;
                    emplaceSourceKey(table, cell.key, it, inserted, cell.hash);
                    AggregateDataPtr & source_place = *cell.mapped;
                    if (inserted)
                    {
                        it->getMapped() = source_place;
                        source_place = nullptr;
                    }
                    else
                    {
                        places.push_back(it->getMapped());
                        source_places.push_back(source_place);
                    }
                }
                merge_source_states();
                for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                {
                    collectPartitionRecords(session, spilled, partition, layout.subOf(partition), scratch.ranges);
                    auto & best_records = scratch.best_records;
                    best_records.clear();
                    forEachArgumentRecord<Key>(
                        table,
                        *adaptive_argument_layout,
                        scratch.ranges,
                        [&](const char * record, size_t bytes, UInt64 hash)
                        {
                            if (is_best(hash))
                                best_records.emplace_back(record, bytes);
                        });
                    if (!best_records.empty())
                        drainAdaptivePartition<Method>(table, arena, best_records, /*alive_bins=*/nullptr, places, scratch.records);
                }
                ProfileEvents::increment(ProfileEvents::AdaptiveAggregationCountFirstUnits);
            }
        }
        else
        {
            /// The sources' cells first: a key a source holds is adopted with its state, so the records of that key
            /// update the adopted state instead of creating one.
            if constexpr (MapAggregationMethod<Method>)
            {
                places.clear();
                source_places.clear();
                for (auto & cell : cells)
                {
                    if (alive_bins && !alive_bins[bucketCountBin(cell.hash)])
                    {
                        discard_cell(cell);
                        continue;
                    }
                    typename Table::LookupResult it;
                    bool inserted = false;
                    emplaceSourceKey(table, cell.key, it, inserted, cell.hash);
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
                        places.push_back(it->getMapped());
                        source_places.push_back(source_place);
                    }
                }
                merge_source_states();
            }
            else
            {
                for (const auto & cell : cells)
                {
                    if (alive_bins && !alive_bins[bucketCountBin(cell.hash)])
                        continue;
                    typename Table::LookupResult it;
                    bool inserted = false;
                    emplaceSourceKey(table, cell.key, it, inserted, cell.hash);
                }
            }

            for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
            {
                collectPartitionRecords(session, spilled, partition, layout.subOf(partition), scratch.ranges);
                pruned_records += drainAdaptivePartition<Method>(table, arena, scratch.ranges, alive_bins, places, scratch.records);
            }
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
            offerTopKCounts(*pruning, *chunk.chunk.getColumns()[params.keys_size + params.bucket_top_k_rank_index]);
        chunks.push_back(std::move(chunk));
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMergeUnits);

        /// The conversion copied the keys out of the records, so the unit's records go now.
        for (const auto & producer : session.producer_buffers)
            for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                producer->releasePartition(partition);
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

    return chunks;
}

void Aggregator::retireAdaptiveMergedBucket(AggregatedDataVariants & dest, size_t bucket) const
{
    dest.adaptive_merge_bucket_arenas[bucket].reset();
    ProfileEvents::increment(ProfileEvents::AdaptiveAggregationBucketsRetired);
}

AggregatedDataVariantsPtr Aggregator::createAdaptiveExternalMergeDestination() const
{
    auto destination = std::make_shared<AggregatedDataVariants>();
    destination->aggregator = this;
    destination->keys_size = params.keys_size;
    destination->key_sizes = key_sizes;
    destination->init(convertToTwoLevelTypeIfPossible(method_chosen));
    destination->adaptive_merge_bucket_arenas.resize(ADAPTIVE_AGGREGATION_NUM_BUCKETS);
    for (auto & slot : destination->adaptive_merge_bucket_arenas)
        slot = std::make_shared<Arena>();
    return destination;
}

void Aggregator::mergeAdaptiveSourceStates(
    AdaptiveMergeScratch & scratch, const AdaptiveAggregationSession & session, Arena * arena, std::atomic<bool> & is_cancelled) const
{
    auto & places = scratch.places;
    auto & source_places = scratch.source_places;
    const size_t merges = places.size();
    chassert(merges == source_places.size());
    chassert(params.max_threads > 0 && session.finished_producers > 0);

    /// A group larger than a worker's share can hold up the merge. Very large states amortize the pool even
    /// when other groups are busy, while small states do not justify its setup even with idle workers.
    const size_t min_parallel_work = std::clamp(
        session.estimated_merge_work / std::min(params.max_threads, session.finished_producers),
        adaptive_parallel_merge_min_work,
        adaptive_parallel_merge_max_work);

    auto & order = scratch.merge_order;
    auto & group = scratch.merge_group;
    bool ordered = false;
    for (size_t i = 0; i < params.aggregates_size; ++i)
    {
        const IAggregateFunction & function = *aggregate_functions[i];
        const size_t offset = offsets_of_aggregate_states[i];
        if (!function.isAbleToParallelizeMerge() || !function.isParallelizeMergePrepareNeeded())
        {
            group.resize(merges);
            for (size_t j = 0; j < merges; ++j)
                group[j] = source_places[j] + offset;
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
                group_work += function.getEstimatedMergeWork(source_places[order[k]] + offset);

            if (group_work <= min_parallel_work)
            {
                for (size_t k = begin; k < end; ++k)
                    function.merge(places[order[k]] + offset, source_places[order[k]] + offset, arena);
            }
            else
            {
                group.clear();
                group.push_back(places[order[begin]] + offset);
                for (size_t k = begin; k < end; ++k)
                    group.push_back(source_places[order[k]] + offset);
                function.parallelizeMergePrepare(group, *thread_pool, is_cancelled);
                function.parallelizeMergeMulti(group, *thread_pool, is_cancelled, arena);
            }
            begin = end;
        }
    }

    /// Source tables retain ownership until every aggregate has merged successfully, so an exception
    /// leaves complete states for their ordinary cleanup. Destruction is non-throwing; the caller clears
    /// the source slots immediately after this call returns.
    for (size_t i = 0; i < params.aggregates_size; ++i)
        if (!aggregate_functions[i]->hasTrivialDestructor())
            aggregate_functions[i]->destroyBatch(0, merges, source_places.data(), offsets_of_aggregate_states[i]);
}

}
