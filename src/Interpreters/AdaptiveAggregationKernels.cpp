/// The method-specialized kernels of the adaptive aggregation: the frozen consume path, the
/// staging of missed rows as partition records, and the partition-wise merge of the staged records.
/// They are member templates of `Aggregator` (defined here rather than in `Aggregator.cpp`,
/// following `ClientBaseOptimizedParts.cpp`), dispatched over the aggregation-method variants.

#include <algorithm>
#include <bit>
#include <limits>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>
#include <Common/memcpySmall.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/unaligned.h>
#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

namespace ProfileEvents
{
    extern const Event AggregationOptimizedEqualRangesOfKeys;
    extern const Event AdaptiveAggregationThaws;
    extern const Event AdaptiveAggregationProbeBypasses;
    extern const Event AdaptiveAggregationStagedRecords;
    extern const Event AdaptiveAggregationStagedBytes;
    extern const Event AdaptiveAggregationDrainedRecords;
    extern const Event AdaptiveAggregationMergeUnits;
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

    template <typename Key>
    ALWAYS_INLINE std::string_view adaptiveStagedKeyBytes(const Key & key)
    {
        if constexpr (std::is_same_v<Key, PackedStringRef>)
            return static_cast<std::string_view>(key);
        else
            return key;
    }

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

    /// Runs `callback` on the row's key bytes while their owner is alive. This is the only
    /// safe shape: a generic hashing state's key holder may own the bytes itself (an
    /// exact-size allocation) or roll its scratch-arena allocation back on discard, so a
    /// pointer must not outlive the holder. States that expose their padded column buffers
    /// skip the holder entirely; fixed-size keys are copied into a local first. The padding
    /// in the ref tells the callback which comparison and copy primitives are legal.
    template <typename SharedKey, typename State, typename Callback>
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
        else if constexpr (adaptive_key_stages_bytes<SharedKey>)
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            callback(KeyBytesRef{adaptiveStagedKeyBytes(keyHolderGetKey(key_holder)), ReadablePadding::Exact});
            keyHolderDiscardKey(key_holder);
        }
        else
        {
            auto && key_holder = state.getKeyHolder(row, scratch);
            const SharedKey widened = keyHolderGetKey(key_holder);
            keyHolderDiscardKey(key_holder);
            callback(KeyBytesRef{std::string_view(reinterpret_cast<const char *>(&widened), sizeof(widened)), ReadablePadding::Exact});
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
    template <typename SharedKey, typename Key>
    void ALWAYS_INLINE recordStagedKey(DB::AdaptiveAggregationProducer & adaptive, const Key & key)
    {
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
        {
            adaptive.miss_key_sizes.push_back(adaptiveStagedKeyBytes(key).size());
        }
        else
        {
            const SharedKey staged = key;
            const char * bytes = reinterpret_cast<const char *>(&staged);
            adaptive.miss_keys.insert(bytes, bytes + sizeof(staged));
        }
    }

    /// Copies a fixed-width argument value; the common widths take a copy of a constant size, which compiles to
    /// a plain load and store instead of a call.
    void ALWAYS_INLINE copyFixedValue(char * to, const char * from, size_t size)
    {
        switch (size)
        {
            case 1: memcpy(to, from, 1); return;
            case 2: memcpy(to, from, 2); return;
            case 4: memcpy(to, from, 4); return;
            case 8: memcpy(to, from, 8); return;
            case 16: memcpy(to, from, 16); return;
            case 32: memcpy(to, from, 32); return;
            default: memcpy(to, from, size); return;
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

    /// Emplaces a staged key into `table` with the record's routing hash. String-like keys were
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

    /// The staged record formats. Every record starts with its routing hash, is padded to 4 bytes
    /// and never straddles two chunks, and nothing in it points outside the record, so a chunk of
    /// records can go to disk and come back byte for byte. The fields are read and written
    /// unaligned, so the padding only keeps a record's size a multiple of the narrowest field.
    constexpr size_t alignStagedRecord(size_t bytes)
    {
        return (bytes + 3) & ~size_t{3};
    }

    /// Count and key-only records: {UInt64 hash, [UInt32 count,] key}. The count is a run length
    /// within one block, which a UInt32 holds. A key whose width varies is preceded by its UInt32
    /// size; a fixed-width key has the compile-time width, so its records have a fixed stride.
    template <typename Key, bool with_count>
    struct StagedKeyRecord
    {
        static constexpr bool variable_width = adaptive_key_stages_bytes<Key>;
        static constexpr size_t size_offset = with_count ? 12 : 8;
        static constexpr size_t key_offset = size_offset + (variable_width ? sizeof(UInt32) : 0);

        static size_t bytes(size_t key_size) { return alignStagedRecord(key_offset + key_size); }

        static void writeHeader(char * record, UInt64 routing_hash, UInt32 count, size_t key_size)
        {
            unalignedStore<UInt64>(record, routing_hash);
            if constexpr (with_count)
                unalignedStore<UInt32>(record + 8, count);
            if constexpr (variable_width)
                unalignedStore<UInt32>(record + size_offset, static_cast<UInt32>(key_size));
        }

        static UInt64 hash(const char * record) { return unalignedLoad<UInt64>(record); }
        static UInt32 count(const char * record) { return unalignedLoad<UInt32>(record + 8); }

        static size_t keySize(const char * record)
        {
            if constexpr (variable_width)
                return unalignedLoad<UInt32>(record + size_offset);
            else
                return sizeof(Key);
        }
    };

    /// General records with a fixed-width key and only fixed-size arguments: {UInt64 hash, key, arguments}, the
    /// arguments laid out by `AdaptiveArgumentLayout`, so every record of the query has the same stride.
    template <typename Key>
    struct StagedFixedArgumentRecord
    {
        static constexpr size_t key_offset = 8;
        static constexpr size_t arguments_offset = key_offset + sizeof(Key);

        static size_t bytes(size_t fixed_argument_bytes) { return alignStagedRecord(arguments_offset + fixed_argument_bytes); }
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

        static size_t bytes(const char * record) { return unalignedLoad<UInt32>(record + 8); }
        static size_t keySize(const char * record) { return unalignedLoad<UInt32>(record + 12); }
    };

    /// Visits every cell of a map table with its key as the table stores it, its mapped value and its hash: from the
    /// iterator, which reads a saved hash instead of rehashing, where the table has one, and through `forEachValue`
    /// for the string table, whose sub-maps share no iterator.
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
            table.forEachValue([&](const auto & key, auto & mapped) { callback(key, mapped, table.hash(key)); });
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

    /// Reads the bucket's spill stream back and removes it. Called by the one task that merges or writes the
    /// bucket, after the finish barrier, so no producer writes to the stream any more.
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
        recordStagedKey<typename SharedMethod::Key>(adaptive, key);
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
            appendDelayedRecords<typename SharedMethod::Key>(
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

    appendDelayedRecords<typename SharedMethod::Key>(
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
                recordStagedKey<typename SharedMethod::Key>(adaptive, key);
            }
            else
            {
                for (size_t i = row_begin; i < row_end; ++i)
                {
                    adaptive.miss_source_rows.push_back(static_cast<UInt32>(i));
                    adaptive.miss_hashes.push_back(hash);
                    recordStagedKey<typename SharedMethod::Key>(adaptive, key);
                }
            }
            appendDelayedRecords<typename SharedMethod::Key>(
                columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/is_simple_count, /*key_row_override=*/0);
        }
        keyHolderDiscardKey(key_holder);
        return;
    }

    if (is_simple_count)
    {
        size_t hits = 0;
        typename SharedMethod::Key last_staged_key{};
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

            const typename SharedMethod::Key staged_key = key;

            bool run_continues = !adaptive.miss_hashes.empty() && adaptive.miss_hashes.back() == hash;
            if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
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
                recordStagedKey<typename SharedMethod::Key>(adaptive, staged_key);

                /// A serialized key view points into the reused scratch arena and can only seed
                /// the run tracking when the views are block-stable; every other key type is
                /// either a self-contained value or, for a packed reference, points into the
                /// block's key column, whose bytes outlive the block.
                if constexpr (std::is_same_v<typename SharedMethod::Key, std::string_view>)
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
        appendDelayedRecords<typename SharedMethod::Key>(columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/true);
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

            recordStagedKey<typename SharedMethod::Key>(adaptive, key);
            keyHolderDiscardKey(key_holder);
        }
        return hits;
    };

    /// Without aggregate functions there is nothing to record for a hit; with the probe bypassed there is no hit.
    if (params.aggregates_size == 0 || bypass_local_probe)
    {
        const size_t hits = probe_rows.template operator()<false>(nullptr);
        update_bypass_sampling(hits, row_end - row_begin);
        appendDelayedRecords<typename SharedMethod::Key>(columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);
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
    appendDelayedRecords<typename SharedMethod::Key>(columns, adaptive, local_find_state, scratch_pool, /*counts_only=*/false);

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


template <typename SharedKey, typename State>
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

    /// A constant key is read from the key columns' single row, while the arguments of each record stay its own
    /// source row.
    const auto key_row_of = [&](size_t record) -> size_t { return key_row_override ? *key_row_override : adaptive.miss_source_rows[record]; };
    const auto key_size_of = [&](size_t record) -> size_t
    {
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
            return adaptive.miss_key_sizes[record];
        else
            return sizeof(SharedKey);
    };

    /// The appends go to random partitions, so the loops below prefetch the cursor of the record
    /// `adaptive_append_cursor_prefetch_distance` ahead and the append position of the one
    /// `adaptive_append_prefetch_distance` ahead, whose cursor is in the cache by then.
    const auto prefetch_append = [&](size_t record) ALWAYS_INLINE
    {
        if (record + adaptive_append_cursor_prefetch_distance < total)
            partitions.prefetchCursor(layout.partitionOf(adaptive.miss_hashes[record + adaptive_append_cursor_prefetch_distance]));
        if (record + adaptive_append_prefetch_distance < total)
            partitions.prefetchAppend(layout.partitionOf(adaptive.miss_hashes[record + adaptive_append_prefetch_distance]));
    };

    const auto write_key = [&](size_t record, size_t key_size, char * to)
    {
        if constexpr (adaptive_key_stages_bytes<SharedKey>)
            withStagedKeyBytes<SharedKey>(
                local_find_state, key_row_of(record), key_size, scratch_pool, [&](const KeyBytesRef & key) { copyStagedKeyBytes(to, key); });
        else
            memcpy(to, adaptive.miss_keys.data() + record * sizeof(SharedKey), sizeof(SharedKey));
    };

    size_t key_bytes = 0;
    size_t variable_argument_bytes = 0;

    const auto append_key_records = [&]<bool with_count>()
    {
        using Record = StagedKeyRecord<SharedKey, with_count>;
        for (size_t i = 0; i < total; ++i)
        {
            prefetch_append(i);
            const UInt64 hash = adaptive.miss_hashes[i];
            const size_t partition = layout.partitionOf(hash);
            const size_t key_size = key_size_of(i);
            char * record = partitions.append(partition, Record::bytes(key_size));
            Record::writeHeader(record, hash, with_count ? adaptive.miss_multiplicities[i] : 0, key_size);
            write_key(i, key_size, record + Record::key_offset);
            partitions.countRecords(partition, 1);
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

        /// The arguments are staged in the form the drain rebuilds and the instructions consume: the representation
        /// wrappers stripped recursively, then `LowCardinality` (see `buildAdaptiveArgumentLayout`).
        Columns arguments(argument_layout.num_positions);
        const auto normalized = [&](size_t position) -> const IColumn &
        {
            if (!arguments[position])
                arguments[position] = recursiveRemoveLowCardinality(columns[position]->convertToFullIfWrapped());
            return *arguments[position];
        };

        struct FixedSource
        {
            const char * values;
            const UInt8 * null_map;
            size_t value_size;
            size_t offset;
        };
        std::vector<FixedSource> fixed_sources;
        fixed_sources.reserve(argument_layout.fixed_fields.size());
        for (const auto & field : argument_layout.fixed_fields)
        {
            const IColumn * values = &normalized(field.position);
            const UInt8 * null_map = nullptr;
            if (const auto * nullable = typeid_cast<const ColumnNullable *>(values))
            {
                null_map = nullable->getNullMapData().data();
                values = &nullable->getNestedColumn();
            }
            fixed_sources.push_back(
                {.values = values->getRawData().data(),
                 .null_map = null_map,
                 .value_size = null_map ? field.size - 1 : field.size,
                 .offset = field.offset});
        }

        std::vector<const IColumn *> variable_sources;
        variable_sources.reserve(argument_layout.variable_fields.size());
        for (const auto & field : argument_layout.variable_fields)
            variable_sources.push_back(&normalized(field.position));

        const auto write_fixed_arguments = [&](char * to, size_t row)
        {
            for (const auto & source : fixed_sources)
            {
                char * field = to + source.offset;
                if (source.null_map)
                    *field++ = static_cast<char>(source.null_map[row]);
                copyFixedValue(field, source.values + row * source.value_size, source.value_size);
            }
        };

        bool fixed_stride = false;
        if constexpr (!adaptive_key_stages_bytes<SharedKey>)
            fixed_stride = variable_sources.empty();

        if (fixed_stride)
        {
            using Record = StagedFixedArgumentRecord<SharedKey>;
            const size_t bytes = Record::bytes(argument_layout.fixed_bytes);
            for (size_t i = 0; i < total; ++i)
            {
                prefetch_append(i);
                const UInt64 hash = adaptive.miss_hashes[i];
                const size_t partition = layout.partitionOf(hash);
                char * record = partitions.append(partition, bytes);
                unalignedStore<UInt64>(record, hash);
                write_key(i, sizeof(SharedKey), record + Record::key_offset);
                write_fixed_arguments(record + Record::arguments_offset, adaptive.miss_source_rows[i]);
                partitions.countRecords(partition, 1);
            }
            key_bytes += total * sizeof(SharedKey);
        }

        /// A value whose serialized size cannot be computed in advance is serialized into the scratch arena first and
        /// copied from there.
        const IColumn::SerializationSettings serialization_settings;
        std::vector<std::optional<std::string_view>> serialized_in_scratch(variable_sources.size());

        for (size_t i = 0; i < total && !fixed_stride; ++i)
        {
            prefetch_append(i);
            const UInt64 hash = adaptive.miss_hashes[i];
            const size_t partition = layout.partitionOf(hash);
            const size_t key_size = key_size_of(i);
            const size_t row = adaptive.miss_source_rows[i];

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

            const size_t bytes = alignStagedRecord(StagedArgumentRecord::header_bytes + argument_layout.fixed_bytes + key_size + variable_bytes);
            char * record = partitions.append(partition, bytes);
            StagedArgumentRecord::writeHeader(record, hash, bytes, key_size);

            char * fixed = record + StagedArgumentRecord::header_bytes;
            write_fixed_arguments(fixed, row);

            /// The key goes before the variable-size arguments, which overwrite whatever its copy scribbled past it.
            char * key = fixed + argument_layout.fixed_bytes;
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

            partitions.countRecords(partition, 1);
            key_bytes += key_size;
            variable_argument_bytes += variable_bytes;
        }
    }

    auto & shared = *adaptive.session;

    /// Thawing is the adaptive aggregation standing down globally: when the staged stream
    /// proves to keep repeating the same missing keys instead of bringing rare ones, every
    /// thread returns to ordinary insertion for good. A frozen table thaws; a thread still
    /// learning stops trying to freeze. Staging such a stream re-copies a repeated key's
    /// bytes on every occurrence, while an unfrozen table would absorb the repeats as cheap
    /// in-place updates.
    ///
    /// The verdict is evaluated over totals shared by all threads; the tuning constants
    /// hold the calibration:
    ///
    ///     wasted bytes per distinct key = (repeat - 1) * bytes per record
    ///                                   > adaptive_thaw_wasted_bytes_per_key
    ///
    /// Here repeat = thaw_sampled_records / distinct_sampled_hashes, and bytes per record =
    /// staged_bytes / staged_records. A key's first record is the price of storing it once;
    /// each repeat wastes one record's bytes, so heavy records tolerate few repeats and tiny
    /// ones many. Until the verdict fires, every batch folds into the shared evidence and
    /// re-evaluates, so the thread whose batch tips the totals over the bound fires for
    /// everyone by setting `thaw_all`, once `staged_records` has reached the
    /// `adaptive_thaw_min_staged_records` evidence floor. A batch updates:
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
    /// - The sampler receives the batch's routing hashes matching `hash & 0xFF == 0`, about
    ///   total / 256 of them, collected outside the lock. `thaw_sampled_records` counts
    ///   every sampled occurrence; `distinct_sampled_hashes` collapses a key's repeats onto
    ///   one entry across all threads, so their ratio estimates the stream's repeat factor
    ///   independently of how the keys spread over the threads.
    ///
    /// The verdict lands at each thread's next between-blocks check; a learning thread about
    /// to freeze also checks it at the crossing, so no table freezes against it. The current
    /// records stay staged: their rows were deferred by the frozen kernel and only the merge
    /// will aggregate them.
    size_t batch_bytes = key_bytes + (counts_only ? total * sizeof(UInt32) : variable_argument_bytes);
    batch_bytes += total * (sizeof(UInt64) + (adaptive_key_stages_bytes<SharedKey> ? sizeof(UInt64) : 0));

    if (!shared.thaw_all.load(std::memory_order_relaxed))
    {
        PaddedPODArray<UInt64> sampled_hashes;
        for (const auto hash : adaptive.miss_hashes)
            if ((hash & adaptive_thaw_sample_mask) == 0)
                sampled_hashes.push_back(hash);

        std::lock_guard lock(shared.thaw_sample_mutex);
        shared.staged_records += total;
        shared.staged_bytes += batch_bytes;
        shared.thaw_sampled_records += sampled_hashes.size();
        for (const auto hash : sampled_hashes)
            shared.distinct_sampled_hashes.insert(hash);
        /// Re-checked under the lock: a thread that sampled while another was firing would
        /// otherwise fire a second time. The verdict compares the wasted staged bytes per
        /// distinct key, (repeat - 1) * bytes per record, against the bound. It is rearranged
        /// onto a common denominator so the arithmetic stays integral:
        /// (sampled - distinct) * staged_bytes > bound * distinct * staged_records.
        /// The products are widened to 128 bits: a giant near-unique stream (billions of
        /// staged records times their bytes) overflows 64, and a wrapped product could thaw
        /// a healthy stream.
        const size_t distinct = shared.distinct_sampled_hashes.size();
        if (!shared.thaw_all.load(std::memory_order_relaxed)
            && shared.staged_records >= adaptive_thaw_min_staged_records
            && shared.thaw_sampled_records > distinct
            && static_cast<UInt128>(shared.thaw_sampled_records - distinct) * shared.staged_bytes
                > static_cast<UInt128>(adaptive_thaw_wasted_bytes_per_key) * distinct * shared.staged_records)
        {
            shared.thaw_all.store(true, std::memory_order_relaxed);
            ProfileEvents::increment(ProfileEvents::AdaptiveAggregationThaws);
            const double repeat = static_cast<double>(shared.thaw_sampled_records) / static_cast<double>(distinct);
            LOG_TRACE(
                log,
                "Adaptive aggregation: thawing the local tables after {} staged records ({} bytes, repeat factor {:.2f}, {} wasted bytes per key)",
                shared.staged_records,
                shared.staged_bytes,
                repeat,
                static_cast<size_t>((repeat - 1.0) * (static_cast<double>(shared.staged_bytes) / static_cast<double>(shared.staged_records))));
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
void NO_INLINE Aggregator::drainAdaptivePartition(
    Table & table,
    Arena * arena,
    const AdaptiveRecordRanges & ranges,
    PaddedPODArray<AggregateDataPtr> & places,
    RowStorePointers & records) const
{
    using Key = typename Method::Key;

    /// Walks the partition's records in order. The table slot of the record
    /// `adaptive_drain_prefetch_look_ahead` positions ahead is prefetched only into a table that
    /// has outgrown the cache, or for string keys, whose emplace compares key bytes behind the
    /// slot; prefetching a cache-resident slot is pure overhead.
    const bool prefetch
        = adaptive_key_stages_bytes<Key> || table.getBufferSizeInBytes() > adaptive_drain_prefetch_min_table_bytes;
    size_t drained = 0;
    /// The callers' lambdas run once per record, so all of them are inlined into the walk.
    const auto walk = [&](auto record_bytes, auto key_of, auto apply) ALWAYS_INLINE
    {
        const auto walk_ranges = [&]<bool with_prefetch>() ALWAYS_INLINE
        {
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
                const auto [key_pos, key_size] = key_of(ahead);
                prefetchStagedKey<Key>(table, key_pos, key_size, unalignedLoad<UInt64>(ahead));
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
                    apply(record);
                    record += record_bytes(record);
                    ++drained;
                }
            }
        };
        if (prefetch)
            walk_ranges.template operator()<true>();
        else
            walk_ranges.template operator()<false>();
    };

    if (is_simple_count)
    {
        using Record = StagedKeyRecord<Key, true>;
        walk(
            [](const char * record) ALWAYS_INLINE { return Record::bytes(Record::keySize(record)); },
            [](const char * record) ALWAYS_INLINE { return std::pair{record + Record::key_offset, Record::keySize(record)}; },
            [&](const char * record) ALWAYS_INLINE
            {
                typename Table::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<Key>(table, record + Record::key_offset, Record::keySize(record), Record::hash(record), it, inserted);
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
            [&](const char * record) ALWAYS_INLINE
            {
                typename Table::LookupResult it;
                bool inserted = false;
                emplaceStagedKey<Key>(table, record + Record::key_offset, Record::keySize(record), Record::hash(record), it, inserted);
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

        bool use_compiled_functions = false;
#if USE_EMBEDDED_COMPILER
        use_compiled_functions = compiled_aggregate_functions_holder != nullptr;
#endif

        places.clear();
        records.ptrs.clear();
        const auto apply = [&](const char * record, const char * key_pos, size_t key_size) ALWAYS_INLINE
        {
            typename Table::LookupResult it;
            bool inserted = false;
            emplaceStagedKey<Key>(table, key_pos, key_size, unalignedLoad<UInt64>(record), it, inserted);
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

        /// The same record shapes the producers chose (see `appendDelayedRecords`).
        bool fixed_stride = false;
        if constexpr (!adaptive_key_stages_bytes<Key>)
            fixed_stride = argument_layout.variable_fields.empty();

        size_t fixed_arguments_offset = StagedArgumentRecord::header_bytes;
        const auto key_of = [&](const char * record) ALWAYS_INLINE
        {
            return std::pair{record + StagedArgumentRecord::header_bytes + argument_layout.fixed_bytes, StagedArgumentRecord::keySize(record)};
        };
        if (fixed_stride)
        {
            using Record = StagedFixedArgumentRecord<Key>;
            const size_t bytes = Record::bytes(argument_layout.fixed_bytes);
            const auto fixed_key_of = [](const char * record) ALWAYS_INLINE { return std::pair{record + Record::key_offset, sizeof(Key)}; };
            walk(
                [bytes](const char *) ALWAYS_INLINE { return bytes; },
                fixed_key_of,
                [&](const char * record) ALWAYS_INLINE { apply(record, record + Record::key_offset, sizeof(Key)); });
            fixed_arguments_offset = Record::arguments_offset;
        }
        else
        {
            walk(
                [](const char * record) ALWAYS_INLINE { return StagedArgumentRecord::bytes(record); },
                key_of,
                [&](const char * record) ALWAYS_INLINE
                {
                    const auto [key_pos, key_size] = key_of(record);
                    apply(record, key_pos, key_size);
                });
        }

        const size_t rows = places.size();
        if (rows)
        {
            /// The arguments are rebuilt into dense columns while the records are hot, so the
            /// ordinary batch executor, and the compiled functions, apply to them as to a block.
            Columns arguments(argument_layout.num_positions);
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
    /// read again.
    if (previous_bucket >= 0)
        std::swap(dest_method.data.impls[bucket], dest_method.data.impls[previous_bucket]);

    using Table = std::decay_t<decltype(dest_method.data.impls[0])>;
    auto & table = dest_method.data.impls[bucket];
    Arena * arena = dest.adaptive_merge_bucket_arenas[bucket].get();

    const AdaptivePartitionLayout layout = session.layout;
    const size_t partitions_per_bucket = layout.partitionsPerBucket();
    const size_t first_partition = static_cast<size_t>(bucket) << layout.sub_bits;

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

        /// The table is empty here, and grows once to hold the unit's records and source cells. The string table
        /// keeps its sub-maps as they are, because it would split the hint evenly over its four size-class sub-maps
        /// while a real key set concentrates in one of them.
        if constexpr (!requires { table.emptyStringSlot(); })
            table.reserve(unit_records + cells.size());

        /// The sources' cells first: a key a source holds is adopted with its state, so the records of that key
        /// update the adopted state instead of creating one.
        if constexpr (MapAggregationMethod<Method>)
        {
            places.clear();
            source_places.clear();
            for (auto & cell : cells)
            {
                typename Table::LookupResult it;
                bool inserted = false;
                table.emplace(cell.key, it, inserted, cell.hash);
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
            for (size_t i = 0; i < params.aggregates_size; ++i)
                aggregate_functions[i]->mergeAndDestroyBatch(
                    places.data(), source_places.data(), places.size(), offsets_of_aggregate_states[i], *thread_pool, is_cancelled, arena);
        }
        else
        {
            for (const auto & cell : cells)
            {
                typename Table::LookupResult it;
                bool inserted = false;
                table.emplace(cell.key, it, inserted, cell.hash);
            }
        }

        for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
        {
            collectPartitionRecords(session, spilled, partition, partition - first_partition, scratch.ranges);
            drainAdaptivePartition<Method>(table, arena, scratch.ranges, places, scratch.records);
        }

        if (full_group_count)
            *full_group_count += table.size();
        if (updater)
            updater->recordAggregationStateSizes(dest, bucket);

        /// Filled by the top-K conversion when the statistics ask for it: the untruncated key bytes, because the
        /// truncated chunk carries only the kept groups.
        UInt64 topk_full_key_bytes = 0;
        auto chunk = convertOneBucketToChunk(dest, arena, final, bucket, updater ? &topk_full_key_bytes : nullptr, /*keep_table_buffer=*/true);
        if (updater)
        {
            if (topk_full_key_bytes)
                updater->recordAggregationKeySizes(chunk.chunk, keys_positions, key_types, topk_full_key_bytes);
            else
                updater->recordAggregationKeySizes(chunk.chunk, keys_positions, key_types);
        }
        chunks.push_back(std::move(chunk));
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMergeUnits);

        /// The conversion copied the keys out of the records, so the unit's records go now.
        for (const auto & producer : session.producer_buffers)
            for (size_t partition = unit_first_partition; partition < unit_first_partition + partitions_per_unit; ++partition)
                producer->releasePartition(partition);
    }

    /// Every source cell of the bucket was adopted or merged and its mapped value nulled, so the
    /// sources' bucket tables only release their buffers.
    for (size_t i = 1; i < data.size(); ++i)
        getDataVariant<Method>(*data[i]).data.impls[bucket].clearAndShrink();

    return chunks;
}

void Aggregator::writeAdaptiveRecordsToTemporaryFiles(AdaptiveAggregationSession & session) const
{
    const auto type = convertToTwoLevelTypeIfPossible(method_chosen);
    const AdaptivePartitionLayout layout = session.layout;
    const size_t partitions_per_bucket = layout.partitionsPerBucket();

    /// A table is written once it holds an eighth of the external-aggregation threshold, never less than a part
    /// worth a file of its own.
    const size_t part_bytes = std::max(params.max_bytes_before_external_group_by / 8, adaptive_external_min_part_bytes);

    const auto create_table = [&]
    {
        auto table = std::make_shared<AggregatedDataVariants>();
        table->aggregator = this;
        table->keys_size = params.keys_size;
        table->key_sizes = key_sizes;
        table->init(type);
        return table;
    };

    auto table = create_table();
    size_t first_unwritten_bucket = 0;
    /// The spilled records of the buckets drained into the current table.
    std::vector<std::vector<SpilledPartitionRecords>> unwritten_spilled;
    /// The written table copied the keys out of the records of its buckets, which go now.
    const auto write_table = [&](size_t end_bucket)
    {
        if (table->hasData())
            consumeToTemporaryFile(*table);
        for (const auto & producer : session.producer_buffers)
            for (size_t partition = first_unwritten_bucket * partitions_per_bucket; partition < end_bucket * partitions_per_bucket; ++partition)
                producer->releasePartition(partition);
        unwritten_spilled.clear();
        first_unwritten_bucket = end_bucket;
        table = create_table();
    };

    PaddedPODArray<AggregateDataPtr> places;
    RowStorePointers records;
    AdaptiveRecordRanges ranges;
    for (size_t bucket = 0; bucket < ADAPTIVE_AGGREGATION_NUM_BUCKETS; ++bucket)
    {
        const auto & spilled = unwritten_spilled.emplace_back(readSpilledBucket(session, bucket));
        visitTwoLevelVariant(
            *table,
            [&](auto & method)
            {
                using Method = std::decay_t<decltype(method)>;
                for (size_t sub = 0; sub < partitions_per_bucket; ++sub)
                {
                    collectPartitionRecords(session, spilled, bucket * partitions_per_bucket + sub, sub, ranges);
                    drainAdaptivePartition<Method>(method.data.impls[bucket], table->aggregates_pool, ranges, places, records);
                }
            });
        if (table->allocatedBytes() >= part_bytes)
            write_table(bucket + 1);
    }
    write_table(ADAPTIVE_AGGREGATION_NUM_BUCKETS);
}

}
