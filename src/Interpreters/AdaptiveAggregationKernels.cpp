/// The method-specialized kernels of the adaptive aggregation: the frozen consume path, the
/// staging of missed rows as partition records, and the partition-wise merge of the staged records.
/// They are member templates of `Aggregator` (defined here rather than in `Aggregator.cpp`,
/// following `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <bit>
#include <limits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/unaligned.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/RowDataStore.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace ProfileEvents
{
    extern const Event AggregationOptimizedEqualRangesOfKeys;
    extern const Event AdaptiveAggregationProbeBypasses;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedBytes;
    extern const Event AdaptiveAggregationDrainedRecords;
    extern const Event AdaptiveAggregationMergeUnits;
    extern const Event AdaptiveAggregationPrunedUnits;
    extern const Event AdaptiveAggregationPrunedRecords;
    extern const Event AdaptiveAggregationCountFirstUnits;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
}

}

namespace
{
    /// String-like keys stage their bytes: a packed reference copied as a plain value would
    /// carry a pointer into the source block, which is released once the block is consumed. The
    /// staged form of both string kinds is the raw characters, and the drain rebuilds the table's
    /// key from them, pointing into the record.
    template <typename Key>
    constexpr bool adaptive_key_stages_bytes = std::is_same_v<Key, std::string_view> || std::is_same_v<Key, PackedStringRef>;

    /// Records retain the key holder's width. A numeric table may widen a key for its cell layout,
    /// but staging and rehashing need only the original value.
    template <typename Method>
    using AdaptiveKeyHolder = decltype(
        std::declval<typename Method::StateNoCache &>().getKeyHolder(0, std::declval<DB::Arena &>()));

    template <typename Method>
    using AdaptiveRecordKey = std::remove_cvref_t<decltype(keyHolderGetKey(std::declval<AdaptiveKeyHolder<Method> &>()))>;

    /// How far past a key's bytes a reader may touch. The overflow-tolerant small copy and
    /// compare primitives access up to 15 bytes past the end, which is only legal for bytes
    /// living in padded containers (column chars, arenas, the chunks of staged records); an
    /// exact-size allocation forbids them.
    enum class ReadablePadding
    {
        Exact,
        AtLeast15Bytes,
    };

    struct KeyBytesRef
    {
        std::string_view bytes;
        ReadablePadding padding;
    };

    /// Runs `callback` on a string-like key's bytes while their owner is alive. A generic hashing state's
    /// key holder may own the bytes itself in an exact-size allocation or roll its scratch-arena allocation
    /// back on discard, so a pointer must not outlive the holder. States that expose their padded column
    /// buffers skip the holder entirely. The padding in the reference tells the callback which comparison
    /// and copy primitives are legal.
    template <typename State, typename Callback>
    void ALWAYS_INLINE withStagedKeyBytes(State & state, size_t row, size_t size, DB::Arena & scratch, Callback && callback)
    {
        /// The fast path requires buffers indexed by the block row directly; the low-cardinality
        /// wrapper inherits `chars`/`offsets` bound to its dictionary (rows go through
        /// `positions`), so it is excluded structurally rather than left to the admission gate.
        if constexpr (requires { state.chars; state.offsets; } && !requires { state.positions; })
        {
            const char * data
                = reinterpret_cast<const char *>(state.chars) + state.offsets[static_cast<ssize_t>(row) - 1];
            callback(KeyBytesRef{std::string_view(data, size), ReadablePadding::AtLeast15Bytes});
        }
        else
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            const auto key = static_cast<std::string_view>(keyHolderGetKey(key_holder));
            callback(KeyBytesRef{key, ReadablePadding::Exact});
            keyHolderDiscardKey(key_holder);
        }
    }

    /// Copy candidate bytes into a staged record, honoring the source's padding. The
    /// overflow-tolerant branch also writes up to 15 bytes past the destination; records are
    /// appended in increasing byte order and every chunk ends in unused padding, so the scribble
    /// lands in space the next append overwrites or that no record uses.
    void ALWAYS_INLINE copyStagedKeyBytes(char * staged, const KeyBytesRef & key)
    {
        if (key.bytes.empty())
            return;
        if (key.padding == ReadablePadding::AtLeast15Bytes && key.bytes.size() <= 64)
            memcpySmallAllowReadWriteOverflow15(staged, key.bytes.data(), key.bytes.size());
        else
            memcpy(staged, key.bytes.data(), key.bytes.size());
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

    /// Emplaces a staged key into `table` with its routing hash. String-like keys were
    /// staged as raw characters and are rebuilt here, pointing into the record: the records of a
    /// partition are freed only after every table holding its keys is converted or written.
    template <typename Key, typename Table>
    void ALWAYS_INLINE emplaceStagedKey(
        Table & table, const char * key_pos, size_t key_size, size_t routing_hash, typename Table::LookupResult & it, bool & inserted)
    {
        if constexpr (std::is_same_v<Key, std::string_view>)
        {
            table.emplace(std::string_view(key_pos, key_size), it, inserted, routing_hash);
        }
        else if constexpr (std::is_same_v<Key, PackedStringRef>)
        {
            /// The staged routing hash IS the packed key's cached content hash
            /// (`DefaultHash<PackedStringRef>` returns it), so the rebuild reuses it instead of
            /// re-hashing the key bytes; `build` consults the functor only for lengths that
            /// store a hash, which is exactly the range the staged hash was derived from.
            const auto key = PackedStringRef::build(
                key_pos, key_size, [routing_hash](const char *, size_t) { return static_cast<UInt32>(routing_hash); });
            table.emplace(key, it, inserted, routing_hash);
        }
        else
        {
            table.emplace(unalignedLoad<Key>(key_pos), it, inserted, routing_hash);
        }
    }

    /// Emplaces the key of a source table's cell into `table` with the cell's hash. The key of a string table is a view
    /// its iteration handed out (see `forEachMappedCellWithHash`), which goes in with `emplaceIteratedKey`.
    template <typename Table, typename Key>
    void ALWAYS_INLINE emplaceSourceKey(Table & table, const Key & key, typename Table::LookupResult & it, bool & inserted, size_t hash)
    {
        if constexpr (requires { table.begin(); })
            table.emplace(key, it, inserted, hash);
        else
            table.emplaceIteratedKey(key, it, inserted, hash);
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

    /// Whether the string views the state's key holders hand out point into storage that
    /// outlives the row loop (a batch-serialized buffer or the key column itself) rather than
    /// into per-row scratch that dies with the holder. Only the serialized methods can go
    /// either way, and they expose the choice as `use_batch_serialize`; the run tracking in
    /// the count kernel may only remember a previous key's view when this holds.
    template <typename State>
    bool ALWAYS_INLINE adaptiveKeyViewsAreBlockStable(const State & state)
    {
        if constexpr (requires { state.use_batch_serialize; })
            return state.use_batch_serialize;
        else
            return true;
    }

    /// The staged record formats. A record never straddles two chunks, and nothing in it points
    /// outside the record, so a chunk can go to disk and come back byte for byte. Fields are read
    /// and written unaligned. Fixed-stride argument records need no padding; the other formats
    /// keep their sizes a multiple of the narrowest header field.
    ///
    /// The merge emplaces a staged key with its routing hash, the table's own hash of the key (see
    /// `emplaceStagedKey`). A record of a byte-staged key, and a general record, starts with the
    /// hash, because rehashing the key bytes would take a pass over them. A record of a fixed stride
    /// has a fixed-width key and stores no hash: the merge rehashes the key in a few instructions,
    /// which costs less than eight more bytes in a record of a dozen or two, written by the staging,
    /// read by the merge, and written to disk and read back by a spill.
    constexpr size_t alignStagedRecord(size_t bytes)
    {
        return (bytes + 3) & ~size_t{3};
    }

    /// Zeroes the padding `alignStagedRecord` put after the last field of a record, which ends at `fields_end`. The merge
    /// never reads it, but a spill writes the records to disk as they are, and a key copied with overflow may have left
    /// bytes of its source there. The padding is shorter than 4 bytes, and the 4 zero bytes may run past the record:
    /// into the next record of the chunk, written after this one, or into the chunk's tail padding.
    void ALWAYS_INLINE zeroStagedRecordPadding(char * fields_end)
    {
        unalignedStore<UInt32>(fields_end, 0);
    }

    /// Count and key-only records: {[UInt64 hash,] [UInt32 count,] [UInt32 size,] key}. The count is
    /// a run length within one block, which a UInt32 holds. A key whose width varies is staged with
    /// the hash and its UInt32 size; a fixed-width key has the compile-time width, so its records
    /// have a fixed stride, and carries no hash.
    template <typename Key, bool with_count>
    struct StagedKeyRecord
    {
        static constexpr bool variable_width = adaptive_key_stages_bytes<Key>;
        static constexpr size_t count_offset = variable_width ? sizeof(UInt64) : 0;
        static constexpr size_t size_offset = count_offset + (with_count ? sizeof(UInt32) : 0);
        static constexpr size_t key_offset = size_offset + (variable_width ? sizeof(UInt32) : 0);

        static size_t bytes(size_t key_size) { return alignStagedRecord(key_offset + key_size); }

        static void writeHeader(char * record, [[maybe_unused]] UInt64 routing_hash, UInt32 count, [[maybe_unused]] size_t key_size)
        {
            if constexpr (variable_width)
            {
                unalignedStore<UInt64>(record, routing_hash);
                unalignedStore<UInt32>(record + size_offset, static_cast<UInt32>(key_size));
            }
            if constexpr (with_count)
                unalignedStore<UInt32>(record + count_offset, count);
        }

        /// The routing hash of the record's key, which `table` hashes as the producer's table did.
        template <typename Table>
        static UInt64 hash([[maybe_unused]] const Table & table, const char * record)
        {
            if constexpr (variable_width)
                return unalignedLoad<UInt64>(record);
            else
                return table.hash(unalignedLoad<Key>(record + key_offset));
        }

        static UInt32 count(const char * record) { return unalignedLoad<UInt32>(record + count_offset); }

        static size_t keySize(const char * record)
        {
            if constexpr (variable_width)
                return unalignedLoad<UInt32>(record + size_offset);
            else
                return sizeof(Key);
        }
    };

    /// General records with a fixed-width key and only fixed-size arguments: {key, arguments}, the arguments laid out
    /// by `AdaptiveArgumentLayout`, so every record of the query has the same stride.
    template <typename Key>
    struct StagedFixedArgumentRecord
    {
        static constexpr size_t key_offset = 0;
        static constexpr size_t arguments_offset = key_offset + sizeof(Key);

        static size_t bytes(size_t fixed_argument_bytes) { return arguments_offset + fixed_argument_bytes; }

        /// The routing hash of the record's key, which `table` hashes as the producer's table did.
        template <typename Table>
        static UInt64 hash(const Table & table, const char * record) { return table.hash(unalignedLoad<Key>(record + key_offset)); }
    };

    /// General records otherwise: {UInt64 hash, UInt32 size, UInt32 key size, fixed-size arguments, key,
    /// variable-size arguments}, the arguments laid out by `AdaptiveArgumentLayout`. `size` is the
    /// whole padded record.
    struct StagedArgumentRecord
    {
        static constexpr size_t header_bytes = 16;

        static void writeHeader(char * record, UInt64 routing_hash, size_t bytes, size_t key_size)
        {
            unalignedStore<UInt64>(record, routing_hash);
            unalignedStore<UInt32>(record + 8, static_cast<UInt32>(bytes));
            unalignedStore<UInt32>(record + 12, static_cast<UInt32>(key_size));
        }

        static UInt64 hash(const char * record) { return unalignedLoad<UInt64>(record); }
        static size_t bytes(const char * record) { return unalignedLoad<UInt32>(record + 8); }
        static size_t keySize(const char * record) { return unalignedLoad<UInt32>(record + 12); }
        static size_t keyOffset(size_t fixed_argument_bytes) { return header_bytes + fixed_argument_bytes; }
    };

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

    /// Adds rows to a count bin of the top-K pruning. The counter saturates, and a saturated bin bounds nothing.
    void ALWAYS_INLINE addToCountBin(UInt16 & bin, UInt64 rows)
    {
        bin = static_cast<UInt16>(std::min<UInt64>(UInt64{bin} + rows, std::numeric_limits<UInt16>::max()));
    }

    /// A count bin's place among the bins of its bucket.
    size_t ALWAYS_INLINE bucketCountBin(UInt64 hash)
    {
        return DB::adaptiveCountBin(hash) & (DB::adaptive_count_bins_per_bucket - 1);
    }

    /// The bounds of a bucket's bins: their sums over the producers, unbounded where a producer's counter saturated.
    std::array<UInt64, DB::adaptive_count_bins_per_bucket> sumBucketCountBins(const DB::AdaptiveTopKPruning & pruning, size_t bucket)
    {
        std::array<UInt64, DB::adaptive_count_bins_per_bucket> bounds{};
        for (const auto & producer : pruning.producer_bins)
        {
            const UInt16 * bins = producer.bins.get() + bucket * DB::adaptive_count_bins_per_bucket;
            for (size_t bin = 0; bin < DB::adaptive_count_bins_per_bucket; ++bin)
            {
                if (bins[bin] == std::numeric_limits<UInt16>::max())
                    bounds[bin] = std::numeric_limits<UInt64>::max();
                else if (bounds[bin] != std::numeric_limits<UInt64>::max())
                    bounds[bin] += bins[bin];
            }
        }
        return bounds;
    }

    /// Offers the counts of a converted unit's groups to the pruning's best counts, and publishes the smallest of them
    /// as the threshold once there are `limit`.
    void offerTopKCounts(DB::AdaptiveTopKPruning & pruning, const DB::IColumn & column)
    {
        const auto & counts = assert_cast<const DB::ColumnUInt64 &>(column).getData();
        std::lock_guard lock(pruning.best_mutex);
        for (const UInt64 count : counts)
        {
            if (pruning.best.size() < pruning.limit)
            {
                pruning.best.push(count);
            }
            else if (count > pruning.best.top())
            {
                pruning.best.pop();
                pruning.best.push(count);
            }
        }
        if (pruning.best.size() == pruning.limit)
            pruning.threshold.store(pruning.best.top(), std::memory_order_relaxed);
    }

    /// The same for a set table, whose cells hold only keys.
    template <typename Table, typename Callback>
    void forEachKeyCellWithHash(Table & table, Callback && callback)
    {
        for (auto it = table.begin(), end = table.end(); it != end; ++it)
            callback(Table::cell_type::getKey(it->getValue()), it.getHash());
    }
}

namespace DB
{

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
        if (!local_method.data.find(key, hash))
        {
            stage_miss(key, hash, row_begin);
            appendDelayedRecords<RecordKey>(
                columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false, /*key_row_override=*/0);
        }
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

    if (!frozen.bypass_local_probe)
    {
        frozen.sampled_hits += hits;
        frozen.sampled_rows += row_end - row_begin;
        if (frozen.sampled_rows >= adaptive_bypass_sample_rows
            && frozen.sampled_hits * adaptive_bypass_hit_rate_inverse < frozen.sampled_rows)
        {
            frozen.bypass_local_probe = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        }
    }

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

    Arena scratch_pool;

    typename LocalMethod::StateNoCache local_find_state(key_columns, key_sizes, aggregation_state_cache);
    /// Routing needs only the two-level twin's TYPE: the hash is the local table's canonical
    /// hash (identical to the twin's by construction - the pairing in `executeFrozen` binds a
    /// method to its own two-level form, which keeps the hash function), and the partitioning
    /// of the hash is static.

    /// The kernel runs only while the producer is frozen, and phase transitions happen between
    /// blocks, so the reference stays valid for the whole block.
    auto & frozen = std::get<AdaptiveAggregationProducer::FrozenState>(adaptive.phase);
    auto update_bypass_sampling = [&](size_t hits, size_t rows)
    {
        if (frozen.bypass_local_probe)
            return;
        frozen.sampled_hits += hits;
        frozen.sampled_rows += rows;
        if (frozen.sampled_rows >= adaptive_bypass_sample_rows
            && frozen.sampled_hits * adaptive_bypass_hit_rate_inverse < frozen.sampled_rows)
        {
            frozen.bypass_local_probe = true;
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationProbeBypasses);
        }
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
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        size_t hits = 0;
        RecordKey last_staged_key{};
        [[maybe_unused]] const bool stable_key_views = adaptiveKeyViewsAreBlockStable(local_find_state);
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
                    ++getInlineCountState(it->getMapped());
                    keyHolderDiscardKey(key_holder);
                    continue;
                }
            }

            const RecordKey staged_key = key;

            bool run_continues = !adaptive.miss_hashes.empty() && adaptive.miss_hashes.back() == hash;
            if constexpr (std::is_same_v<RecordKey, std::string_view>)
                run_continues = run_continues && stable_key_views && staged_key == last_staged_key;
            else
                run_continues = run_continues && staged_key == last_staged_key;

            if (run_continues)
            {
                ++adaptive.miss_multiplicities.back();
            }
            else
            {
                adaptive.miss_hashes.push_back(hash);
                adaptive.miss_multiplicities.push_back(1);
                /// Fixed-size keys stage no size: it is a compile-time constant the publish
                /// substitutes, so the hot staging loop skips a dead store per record.
                recordStagedKey<RecordKey>(adaptive, staged_key);

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is
                /// either a self-contained value or, for a packed reference, points into the
                /// block's key column, whose bytes outlive the block.
                if constexpr (std::is_same_v<RecordKey, std::string_view>)
                {
                    if (stable_key_views)
                        last_staged_key = staged_key;
                }
                else
                {
                    last_staged_key = staged_key;
                }
                adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
            }
            keyHolderDiscardKey(key_holder);
        }
        update_bypass_sampling(hits, row_end - row_begin);
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
        update_bypass_sampling(hits, row_end - row_begin);
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
    update_bypass_sampling(hits, row_end - row_begin);
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
    const AdaptivePartitionLayout layout = partitions.layout();
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
            withStagedKeyBytes(
                local_find_state, key_row_of(record), key_size, scratch_pool, [&](const KeyBytesRef & key) { copyStagedKeyBytes(to, key); });
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

        /// A value whose serialized size cannot be computed in advance is serialized into the scratch arena first and
        /// copied from there.
        const IColumn::SerializationSettings serialization_settings;
        std::vector<std::optional<std::string_view>> serialized_in_scratch(variable_sources.size());

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
                    for (size_t j = 0; j < variable_sources.size(); ++j)
                    {
                        if (const auto size = variable_sources[j]->getSerializedValueSize(row, &serialization_settings))
                        {
                            serialized_in_scratch[j].reset();
                            variable_bytes += *size;
                        }
                        else
                        {
                            const char * begin = nullptr;
                            serialized_in_scratch[j] = variable_sources[j]->serializeValueIntoArena(row, scratch_pool, begin, &serialization_settings);
                            variable_bytes += serialized_in_scratch[j]->size();
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
                    for (size_t j = 0; j < variable_sources.size(); ++j)
                    {
                        if (const auto & serialized = serialized_in_scratch[j])
                        {
                            memcpy(variable, serialized->data(), serialized->size());
                            variable += serialized->size();
                        }
                        else
                            variable = variable_sources[j]->serializeValueIntoMemory(row, variable, &serialization_settings);
                    }
                    zeroStagedRecordPadding(variable);

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

    /// The rows behind the records go to the count bins of the top-K pruning: a count record stands for its run.
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
    ///   count batch stages a run length instead of arguments.
    ///   Variable-width arguments count in full because staging such a value pays real work
    ///   at every step. The record copies it out of the block, the record pins that memory
    ///   until the merge drains it, and updating the aggregate state from it copies the value
    ///   once more (a string min keeps its own copy of the winning value). A repeated key pays
    ///   all of that on every occurrence, where an unfrozen table would have paid a single
    ///   in-place state update, so each repeat of a heavy value is genuine waste.
    ///   Fixed-width arguments are deliberately not counted because their staging copy is a
    ///   few bytes and the drain consumes them with the same vectorized batch executor the
    ///   scan would have used on the original block. Deferring such values moves the work
    ///   without multiplying it, so their staging costs about what their consumption saves.
    ///   Charging them would fire the thaw on streams where staging is in fact profitable. The
    ///   measured anchor is a stream of five UInt64 arguments at repeat 10: it stays a clear
    ///   adaptive win, and counting its forty fixed bytes per record would have thawed it.
    /// - The sample receives routing hashes matching `hash & 0xFF == 0`. Each distinct key has the
    ///   same sampling probability, and `distinct_sampled_hashes` collapses its repeats onto one entry.
    ///   For a growing set aggregate, a bounded sketch also counts distinct argument tuples of these
    ///   keys, including the key in their hash because equal arguments in different groups occupy
    ///   separate states.
    if (adaptiveMayThaw(*adaptive.session))
    {
        chassert(!adaptive_state_bytes_per_distinct_input || aggregates_positions.size() == 1);
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
                    for (const auto position : aggregates_positions[0])
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

template <typename Method, typename Table>
size_t NO_INLINE Aggregator::drainAdaptivePartition(
    Table & table,
    Arena * arena,
    const AdaptiveRecordRanges & ranges,
    const bool * alive_bins,
    PaddedPODArray<AggregateDataPtr> & places,
    RowStorePointers & records,
    bool count_only) const
{
    using Key = AdaptiveRecordKey<Method>;

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

        if (count_only)
        {
            const auto count = [&](const char * key_pos, size_t key_size, UInt64 hash) ALWAYS_INLINE
            {
                typename Table::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<Key>(table, key_pos, key_size, hash, it, inserted);
                if (inserted)
                    getInlineCountState(it->getMapped()) = 1;
                else
                    ++getInlineCountState(it->getMapped());
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
    auto & table = dest_method.data.impls[bucket];
    Arena * arena = dest.adaptive_merge_bucket_arenas[bucket].get();

    const AdaptivePartitionLayout layout = session.layout;
    const size_t partitions_per_bucket = layout.partitionsPerBucket();
    const size_t first_partition = static_cast<size_t>(bucket) * partitions_per_bucket;

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
    const auto unit_of = [&](size_t hash) { return (layout.partitionOf(hash) & (partitions_per_bucket - 1)) >> unit_shift; };
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
    const auto better = [ascending = params.bucket_top_k_ascending](UInt64 a, UInt64 b) { return ascending ? a < b : a > b; };
    /// The table of a count-first unit's best groups, which takes the bucket's slot for the conversion while the slot's
    /// table, grown by the counting, waits for the next unit.
    Table best_groups_table;

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
        if (count_first)
        {
            if constexpr (MapAggregationMethod<Method>)
            {
                using Key = AdaptiveRecordKey<Method>;

                /// The first pass counts the groups in the table, each count kept in the mapped value as a lone
                /// `count()` keeps it: a source cell adds its count state, a record one row.
                for (const auto & cell : cells)
                {
                    if (alive_bins && !alive_bins[bucketCountBin(cell.hash)])
                        continue;
                    typename Table::LookupResult it;
                    bool inserted = false;
                    emplaceSourceKey(table, cell.key, it, inserted, cell.hash);
                    const UInt64 count = getCountState(*cell.mapped + rank_offset);
                    if (inserted)
                        getInlineCountState(it->getMapped()) = count;
                    else
                        getInlineCountState(it->getMapped()) += count;
                }
                for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                {
                    collectPartitionRecords(session, spilled, partition, partition - first_partition, scratch.ranges);
                    pruned_records += drainAdaptivePartition<Method>(
                        table, arena, scratch.ranges, alive_bins, places, scratch.records, /*count_only=*/true);
                }
                unit_groups = table.size();

                /// The unit's best groups by their counts, which are exact: the unit holds every record and source cell
                /// of its keys. With the pruning, a group counted below the threshold cannot reach the top. Only the
                /// cells that enter the heap are hashed.
                const UInt64 threshold = pruning ? pruning->threshold.load(std::memory_order_relaxed) : 0;
                auto & best = scratch.best_counts_and_hashes;
                best.clear();
                const auto worse_first = [&](const auto & lhs, const auto & rhs) { return better(lhs.first, rhs.first); };
                forEachMappedCellWithHashOnDemand(
                    table,
                    [&](AggregateDataPtr & mapped, const auto & hash_of)
                    {
                        const UInt64 count = getInlineCountState(mapped);
                        if (count < threshold)
                            return;
                        if (best.size() < params.bucket_top_k)
                        {
                            best.emplace_back(count, hash_of());
                            std::push_heap(best.begin(), best.end(), worse_first);
                        }
                        else if (better(count, best.front().first))
                        {
                            std::pop_heap(best.begin(), best.end(), worse_first);
                            best.back() = {count, hash_of()};
                            std::push_heap(best.begin(), best.end(), worse_first);
                        }
                    });
                auto & best_hashes = scratch.best_hashes;
                best_hashes.clear();
                for (const auto & [count, hash] : best)
                    best_hashes.push_back(hash);
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

                /// The second pass builds the states of the best groups, in the table that takes the bucket's slot for the
                /// conversion: their source cells are adopted or merged and their records drained as in the ordinary
                /// merge, and the other source cells go with their states.
                table.clear();
                std::swap(table, best_groups_table);
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
                    }
                    else
                    {
                        places.push_back(it->getMapped());
                        source_places.push_back(source_place);
                    }
                    source_place = nullptr;
                }
                mergeAdaptiveSourceStates(scratch, session, arena, is_cancelled);
                for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                {
                    collectPartitionRecords(session, spilled, partition, partition - first_partition, scratch.ranges);
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
                    }
                    else if (inserted)
                    {
                        it->getMapped() = source_place;
                    }
                    else
                    {
                        places.push_back(it->getMapped());
                        source_places.push_back(source_place);
                    }
                    source_place = nullptr;
                }
                mergeAdaptiveSourceStates(scratch, session, arena, is_cancelled);
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
                collectPartitionRecords(session, spilled, partition, partition - first_partition, scratch.ranges);
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
        if (count_first)
            std::swap(table, best_groups_table);
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

    return chunks;
}

}
