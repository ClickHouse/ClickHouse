#include <Processors/Transforms/RadixUniqExactTransform.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/logger_useful.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/getL2CacheSize.h>
#include <base/unaligned.h>
#include <Interpreters/HashTablesStatistics.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Processors/RowsBeforeStepCounter.h>

#include <array>
#include <bit>
#include <cstring>
#include <optional>


/// Accounts the allocations of the state to its tracker (see `IRadixUniqExactState::getMemoryTracker`). Only the calls
/// into the state are wrapped: the input chunk was allocated under the tracker of the query, so freeing it here would
/// be charged to the state.
#define RADIX_UNIQ_EXACT_TRACK_MEMORY(state_) \
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher; \
    if ((state_)->useOwnTracker()) \
        memory_tracker_switcher.emplace((state_)->getMemoryTracker())

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The bucket of a key is selected by the top bits of `intHash64`, the finalizer of MurmurHash3:
/// - Every bit of the key affects every bit of the hash, so the keys that differ only in their low bits, such as
///   sequential identifiers or timestamps, are spread evenly over the buckets. The top bits of the key itself, or of
///   a weak hash, would send them all to a few buckets, and the build of these buckets would not be balanced.
/// - `intHashCRC32` is not good enough for it: it has only 32 bits, and they fail the avalanche and bit independence
///   tests of SMHasher (see the comment on CRC32C in `Common/HashTable/Hash.h`).
/// - It is cheap (two multiplications) and computed only once per routed key.
ALWAYS_INLINE inline UInt64 bucketHash(UInt64 x)
{
    return intHash64(x);
}

ALWAYS_INLINE inline UInt64 bucketHash(UInt32 x)
{
    return intHash64(x);
}

/// The 128-bit keys are uniformly distributed (see `mixKey`), so their low half is used as the hash, and the high half
/// is used for the slot.
ALWAYS_INLINE inline UInt64 bucketHash(UInt128 x)
{
    return static_cast<UInt64>(x);
}

/// The slot of a key in a set is selected by the top bits of a different hash:
/// - The keys of a bucket share the top bits of `bucketHash`, so a hash that is independent of it is needed to
///   spread them over the whole set.
/// - It is computed for every insertion into a set (the local set, the deduplication of the buffered keys, the build
///   of a bucket), so it must be as cheap as possible. It is Fibonacci hashing (Knuth, TAOCP vol. 3, 6.4): the
///   multiplication by 2^64 divided by the golden ratio, a single instruction. Its top bits depend on all the bits of
///   the key, and it spreads a range of consecutive keys over the set almost without collisions, whatever the size
///   of the set.
ALWAYS_INLINE inline UInt64 slotHash(UInt64 x)
{
    return x * 0x9E3779B97F4A7C15ULL;
}

ALWAYS_INLINE inline UInt64 slotHash(UInt32 x)
{
    return slotHash(UInt64{x});
}

ALWAYS_INLINE inline UInt64 slotHash(UInt128 x)
{
    return static_cast<UInt64>(x >> 64);
}

/// A bijection that makes the 128-bit values that are not hashes (`UUID`, `UInt128`, `Int128`) uniformly distributed,
/// so that both halves of the result can serve as hashes. It is a two-round Feistel network with `intHash64` as the
/// round function: each round only XORs one half with a function of the other one, so it can be undone, and distinct
/// values stay distinct.
ALWAYS_INLINE inline UInt128 mixKey(UInt128 x)
{
    UInt64 low = static_cast<UInt64>(x);
    UInt64 high = static_cast<UInt64>(x >> 64) ^ bucketHash(low);
    low ^= bucketHash(high);
    return (UInt128(high) << 64) | low;
}

/// When a set does not fit into the L2 cache, the cell of the key this far ahead is prefetched while a key is inserted,
/// so that the cache misses of the inserts overlap.
constexpr size_t PREFETCH_DISTANCE = 32;

template <typename Key>
struct SlotHash
{
    size_t operator()(Key key) const { return slotHash(key); }
};

/// Linear probing with the slot selected by the top bits of the hash (see `slotHash`). The hash table never grows by
/// itself: the load factor of 0.5 is kept by the callers, which rebuild the set (see `growLocalSet`).
struct SlotGrower : HashTableGrower<1>
{
    size_t place(size_t x) const { return x >> (64 - size_degree); }
    bool overflow(size_t /*elems*/) const { return false; }
};

/// A `HashSet` with the interface the streams and the buckets use.
template <typename Key>
class RadixSet
{
    using Set = HashSetTable<Key, HashTableCell<Key, SlotHash<Key>>, SlotHash<Key>, SlotGrower, HashTableAllocator>;

public:
    /// Empties the set and makes its capacity `capacity`, a power of two.
    void reset(size_t capacity)
    {
        chassert(std::has_single_bit(capacity));
        if (capacity == set.getBufferSizeInCells())
        {
            set.clear();
            return;
        }
        set = Set();
        set.reserve(capacity / 2);
    }

    /// Returns true if the key was not in the set.
    ALWAYS_INLINE bool insert(Key key)
    {
        typename Set::LookupResult it;
        bool inserted = false;
        set.emplace(key, it, inserted);
        return inserted;
    }

    /// Inserts the keys, which must fit without exceeding the load factor of 0.5.
    template <bool prefetch>
    void insertMany(const Key * keys, size_t num_keys)
    {
        chassert((numCellsInUse() + num_keys) * 2 <= capacity());
        set.template insertManyWithoutResize<prefetch ? PREFETCH_DISTANCE : 0>(keys, num_keys);
    }

    /// Inserts the keys, choosing whether to prefetch by the size of the set.
    void insertMany(const Key * keys, size_t num_keys)
    {
        if (capacity() * sizeof(Key) > getL2CacheSize())
            insertMany<true>(keys, num_keys);
        else
            insertMany<false>(keys, num_keys);
    }

    size_t size() const { return set.size(); }
    size_t capacity() const { return set.getBufferSizeInCells(); }
    size_t numCellsInUse() const { return set.size() - set.hasZero(); }

    template <typename Func>
    void forEach(Func && func) const
    {
        for (const auto & cell : set)
            func(cell.getKey());
    }

private:
    Set set;
};

/// A stream switches from its local set to the routing once the set would outgrow this size.
constexpr size_t MAX_LOCAL_SET_BYTES = 1024 * 1024;

/// A stream deduplicates its buffered keys once it has buffered this many keys for the first time.
constexpr size_t INITIAL_DEDUPLICATION_BUDGET = 64 * 1024;

/// The number of buffered keys of a stream, and the number of buffers, that are deduplicated first to estimate whether
/// the deduplication of all of them is worth it.
constexpr size_t DEDUPLICATION_SAMPLE_KEYS = 4096;
constexpr size_t DEDUPLICATION_SAMPLE_BUFFERS = 8;

/// After a deduplication leaves N keys in the buffers of a stream, the next one starts when the buffers hold N times this
/// factor. For example, if 10000 distinct keys are left, the next deduplication comes at 80000 buffered keys, after 70000
/// more keys are routed. Each deduplication inserts all the buffered keys again, so a large factor deduplicates the
/// repeated keys less often, at the cost of memory.
constexpr size_t DEDUPLICATION_BUDGET_FACTOR = 8;

/// The first deduplication after the switch to the routing comes sooner, because it decides whether the stream goes
/// back to its local set, and the keys routed until then are wasted work if it does.
constexpr size_t FIRST_DEDUPLICATION_BUDGET_FACTOR = 4;

/// A stream whose keys are mostly repeated goes back to inserting them into its local set only if the set would not
/// be larger than this. Larger sets do not fit into the CPU caches, and the inserts into them are slower than routing
/// and deduplicating the keys in cache-resident sets, even if every key is deduplicated several times.
constexpr size_t MAX_UNBOUNDED_SET_BYTES = 4 * 1024 * 1024;

/// The number of buckets per stream. Many buckets keep the bucket sets cache-resident even when there are
/// many distinct keys, and they balance the build between the threads.
constexpr size_t BUCKETS_PER_STREAM = 16;

/// A bucket whose set would be larger than this is partitioned again when it is built (see `buildSplitBucket`): the
/// builders fill their sets at the same time, and large sets do not fit into the caches together.
constexpr size_t MAX_BUCKET_SET_BYTES = 2 * 1024 * 1024;

/// The keys of a part are inserted by chunks, so that its set grows with the distinct keys rather than all the keys.
constexpr size_t SPLIT_INSERT_CHUNK = 1024;

/// At least 256 buckets, and at most 4096, because every stream keeps a buffer per bucket.
constexpr size_t MIN_BUCKET_BITS = 8;
constexpr size_t MAX_BUCKET_BITS = 12;

/// The blocks of a buffer of buffered keys grow from `MIN_BLOCK_SIZE` to `MIN_BLOCK_SIZE << MAX_BLOCK_CLASS` keys,
/// so that the buffers of the buckets with few keys stay small.
constexpr size_t MIN_BLOCK_SIZE = 16;
constexpr size_t MAX_BLOCK_CLASS = 6;

/// The arena of the blocks of a stream grows linearly past this size: a stream holds tens of MiB, and the doubling of
/// the default arena leaves up to half of its last chunk unused.
constexpr size_t ARENA_LINEAR_GROWTH_THRESHOLD = 1024 * 1024;

/// The rows of one vector register are checked at once to be all equal to the previous row, and skipped then.
#if defined(__AVX512BW__)
constexpr size_t DUPLICATES_BLOCK_BYTES = 64;
#elif defined(__AVX2__)
constexpr size_t DUPLICATES_BLOCK_BYTES = 32;
#else
constexpr size_t DUPLICATES_BLOCK_BYTES = 16; /// SSE2, NEON
#endif

template <typename Key>
class RadixUniqExactState final : public IRadixUniqExactState
{
public:
    RadixUniqExactState(
        size_t num_streams_, size_t bucket_bits_, const StatsCollectingParams & stats_collecting_params_, std::optional<RadixUniqExactEntry> hint_)
        : bucket_bits(bucket_bits_)
        , num_buckets(1ULL << bucket_bits_)
        , streams(num_streams_)
        , stats_collecting_params(stats_collecting_params_)
        , hint(hint_)
    {
        /// The sets preallocated from the statistics are the memory of the state.
        RADIX_UNIQ_EXACT_TRACK_MEMORY(this);
        for (auto & stream : streams)
        {
            stream.local_set.reset(INITIAL_LOCAL_SET_CAPACITY);
            if (hint)
                startFromHint(stream);
        }
    }

    /// Inserts the keys of the rows (after skipping NULLs and consecutive duplicates) of the column. `extract` maps
    /// the row number to the key, `same(last_row, row)` tells whether the key of `row` is known to be routed already,
    /// such as when the row equals the last routed row. The rows are compared before the keys are computed, because
    /// computing a key (hashing a string) is more expensive.
    template <typename Extract, typename Same>
    ALWAYS_INLINE void routeRows(size_t stream_index, size_t num_rows, const UInt8 * null_map, Extract && extract, Same && same)
    {
        auto & stream = streams[stream_index];
        std::array<Key, KEYS_BATCH_SIZE> keys; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - only the first num_keys entries are read
        size_t last_row = 0;
        bool has_last_row = false;

        for (size_t begin = 0; begin < num_rows; begin += keys.size())
        {
            const size_t end = std::min(begin + keys.size(), num_rows);
            size_t num_keys = 0;
            for (size_t row = begin; row < end; ++row)
            {
                if (null_map && null_map[row])
                    continue;

                if (has_last_row && same(last_row, row))
                    continue;
                last_row = row;
                has_last_row = true;

                keys[num_keys++] = extract(row);
            }

            insertBatch(stream, keys.data(), num_keys);
        }

        rows.fetch_add(num_rows, std::memory_order_relaxed);
    }

    /// The same as `routeRows` for the values that are the keys themselves (zero-extended). NULLs and consecutive
    /// duplicates are dropped without branches into a buffer, which is then inserted. As in the ordinary `uniqExact`,
    /// the consecutive duplicates are looked for only while they are frequent.
    /// The values are loaded from the bytes, as the column may hold another type of the same size (e.g. `Float64`).
    template <typename T>
    void routeValues(size_t stream_index, const char * bytes, size_t num_rows, const UInt8 * null_map)
    {
        auto data = [bytes](size_t row) { return unalignedLoad<T>(bytes + row * sizeof(T)); };
        auto & stream = streams[stream_index];
        std::array<Key, KEYS_BATCH_SIZE> keys; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - only the first num_keys entries are read
        T last_value{};
        bool has_last_value = false;
        bool skip_duplicates = true;
        size_t num_seen = 0;
        size_t num_skipped = 0;

        for (size_t begin = 0; begin < num_rows; begin += keys.size())
        {
            const size_t end = std::min(begin + keys.size(), num_rows);
            size_t num_keys = 0;
            if (null_map)
            {
                for (size_t row = begin; row < end; ++row)
                {
                    const T value = data(row);
                    const bool is_null = null_map[row];
                    keys[num_keys] = static_cast<Key>(value);
                    num_keys += !is_null && !(skip_duplicates && has_last_value && value == last_value);
                    last_value = is_null ? last_value : value;
                    has_last_value |= !is_null;
                }
            }
            else if (skip_duplicates)
            {
                /// The rows of a column are contiguous, so a row is compared with the previous one in memory, and
                /// the only dependency between the iterations is the number of keys.
                /// A block that repeats the previous row (long runs, e.g. a sorted `CounterID`) is skipped with
                /// vector compares, as the per-row stores would cost 1.85x the ordinary `uniqExact` there.
                constexpr size_t block_size = DUPLICATES_BLOCK_BYTES / sizeof(T);
                for (size_t block = begin; block < end; block += block_size)
                {
                    const size_t block_end = std::min(block + block_size, end);
                    if (block != 0 && block_end - block == block_size)
                    {
                        bool all_same = true;
                        for (size_t i = 0; i < block_size; ++i)
                            all_same &= data(block + i) == data(block + i - 1);
                        if (all_same)
                            continue;
                    }

                    for (size_t row = block; row < block_end; ++row)
                    {
                        keys[num_keys] = static_cast<Key>(data(row));
                        num_keys += row == 0 || data(row) != data(row - 1);
                    }
                }
            }
            else
            {
                for (size_t row = begin; row < end; ++row)
                    keys[num_keys++] = static_cast<Key>(data(row));
            }

            if (skip_duplicates)
            {
                num_seen += end - begin;
                num_skipped += end - begin - num_keys;
                skip_duplicates = num_skipped * 4 > num_seen;
            }

            insertBatch(stream, keys.data(), num_keys);
        }

        rows.fetch_add(num_rows, std::memory_order_relaxed);
    }

    void route(size_t stream_index, const IColumn & column, const UInt8 * null_map) override;

    void finishStream(size_t stream_index) override
    {
        auto & stream = streams[stream_index];
        stream.ended_in_set = stream.mode != Mode::Routing;
        /// The local set holds distinct keys, so no buffer needs a deduplication while it is moved.
        if (stream.mode != Mode::Routing)
        {
            stream.estimated_distinct = stream.local_set.size();
            switchToRouting(stream);
        }
        else
        {
            /// Estimated from a sample of buffers (see the statistics in `RadixUniqExactTransform.h`).
            size_t sampled_keys = 0;
            size_t sampled_distinct = 0;
            size_t num_sampled = 0;
            syncBuffers(stream);
            for (; num_sampled < num_buckets && (sampled_keys < DEDUPLICATION_SAMPLE_KEYS || num_sampled < DEDUPLICATION_SAMPLE_BUFFERS);
                 ++num_sampled)
            {
                auto & buffer = stream.local_buffers[num_sampled];
                sampled_keys += buffer.size;
                stream.num_buffered -= buffer.size;
                deduplicateBuffer(stream, num_sampled);
                stream.num_buffered += buffer.size;
                sampled_distinct += buffer.size;
            }
            stream.estimated_distinct = sampled_distinct * num_buckets / std::max<size_t>(num_sampled, 1);
        }

        /// The build reads only the buffers, so the set must not hold memory while the bucket sets are built.
        stream.local_set = {};
    }

    size_t buildBucket(size_t bucket) override
    {
        size_t total = 0;
        for (const auto & stream : streams)
        {
            if (stream.mode != Mode::Routing)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A bucket is built before all the streams are routed");
            total += stream.local_buffers[bucket].size;
        }

        /// Counted before the early return of an empty bucket, the statistics need every bucket to be built.
        num_built_buckets.fetch_add(1, std::memory_order_relaxed);
        if (total == 0)
            return 0;

        /// The load factor is at most 0.5, the actual number of distinct keys can only be lower. With the statistics, the
        /// set is allocated for the expected number of distinct keys of a bucket, with a margin for the variation
        /// between the buckets, and it grows if there are more of them.
        size_t expected = total;
        if (hint)
        {
            const size_t bucket_distinct = hint->num_distinct / num_buckets;
            expected = std::min(total, bucket_distinct + bucket_distinct / 16 + MIN_BLOCK_SIZE);
        }

        const size_t set_bytes = std::bit_ceil(expected * 2) * sizeof(Key);
        if (set_bytes > MAX_BUCKET_SET_BYTES)
            return buildSplitBucket(bucket, total, expected, std::countr_zero(set_bytes / MAX_BUCKET_SET_BYTES));

        RadixSet<Key> bucket_set;
        bucket_set.reset(std::bit_ceil(expected * 2));

        for (const auto & stream : streams)
        {
            for (const Block * block = stream.local_buffers[bucket].head; block; block = block->next)
            {
                while ((bucket_set.numCellsInUse() + block->size) * 2 > bucket_set.capacity())
                    growLocalSet(bucket_set);
                bucket_set.insertMany(block->keys(), block->size);
            }
        }

        return bucket_set.size();
    }

    /// Builds a bucket whose set would not fit into the caches: its keys are partitioned again by the next bits of
    /// `bucketHash`, and a small set is built for each part. This is the multi-pass radix partitioning of Boncz, Manegold,
    /// Kersten, "Database Architecture Optimized for the New Bottleneck: Memory Access" (VLDB 1999).
    size_t buildSplitBucket(size_t bucket, size_t total, size_t expected, size_t split_bits)
    {
        const size_t num_parts = size_t{1} << split_bits;
        std::vector<PODArray<Key>> parts(num_parts);
        for (auto & part : parts)
            part.reserve(total / num_parts + total / (8 * num_parts) + MIN_BLOCK_SIZE);

        const size_t shift = 64 - split_bits;
        for (const auto & stream : streams)
        {
            for (const Block * block = stream.local_buffers[bucket].head; block; block = block->next)
            {
                for (size_t i = 0; i < block->size; ++i)
                {
                    const Key key = block->keys()[i];
                    parts[(bucketHash(key) << bucket_bits) >> shift].push_back(key);
                }
            }
        }

        const size_t part_expected = expected / num_parts + expected / (16 * num_parts) + MIN_BLOCK_SIZE;
        size_t count = 0;
        RadixSet<Key> part_set;
        for (const auto & part : parts)
        {
            if (part.empty())
                continue;

            part_set.reset(std::bit_ceil(std::min(part.size(), part_expected) * 2));
            for (size_t begin = 0; begin < part.size(); begin += SPLIT_INSERT_CHUNK)
            {
                const size_t num_keys = std::min(SPLIT_INSERT_CHUNK, part.size() - begin);
                while ((part_set.numCellsInUse() + num_keys) * 2 > part_set.capacity())
                    growLocalSet(part_set);
                part_set.insertMany(part.data() + begin, num_keys);
            }
            count += part_set.size();
        }
        return count;
    }

    void recordStatistics(size_t num_distinct) override
    {
        /// A run without keys tells nothing about the next one: it would start every stream in an unbounded set.
        if (!stats_collecting_params.isCollectionAndUseEnabled() || num_built_buckets.load(std::memory_order_relaxed) != num_buckets
            || num_distinct == 0)
            return;

        std::vector<size_t> stream_distinct;
        std::vector<size_t> stream_keys;
        size_t num_ended_in_set = 0;
        for (const auto & stream : streams)
        {
            stream_distinct.push_back(stream.estimated_distinct);
            stream_keys.push_back(stream.num_routed);
            num_ended_in_set += stream.ended_in_set;
        }

        auto median = [](std::vector<size_t> & values)
        {
            auto middle = values.begin() + values.size() / 2;
            std::nth_element(values.begin(), middle, values.end());
            return *middle;
        };

        getHashTablesStatistics<RadixUniqExactEntry>().update(
            RadixUniqExactEntry{
                .num_distinct = num_distinct,
                .stream_distinct = median(stream_distinct),
                .stream_keys = median(stream_keys),
                .num_streams = streams.size(),
                .streams_ended_in_set = num_ended_in_set * 2 > streams.size()},
            stats_collecting_params);
    }

    size_t numBuckets() const override { return num_buckets; }

private:
    static constexpr size_t INITIAL_LOCAL_SET_CAPACITY = 256;

    /// The keys of a column are computed into a buffer of this size, which is then inserted.
    static constexpr size_t KEYS_BATCH_SIZE = 256;

    enum class Mode : uint8_t
    {
        /// The keys are inserted into the local set, until it outgrows the cache.
        BoundedSet,
        /// The keys are buffered in the buffers of the buckets.
        Routing,
        /// The keys are inserted into the local set, which grows without a limit.
        UnboundedSet,
    };

    /// The header of a block, followed by its keys.
    struct Block
    {
        Block * next;
        UInt32 size;
        /// The capacity is `MIN_BLOCK_SIZE << size_class`.
        UInt32 size_class;

        size_t capacity() const { return MIN_BLOCK_SIZE << size_class; }
        Key * keys() { return reinterpret_cast<Key *>(this + 1); }
        const Key * keys() const { return reinterpret_cast<const Key *>(this + 1); }
    };

    /// The keys follow the header, aligned.
    static_assert(sizeof(Block) % alignof(Key) == 0);

    /// Where the next key of a buffer is written. It is all that routing a key touches besides the key's slot, so the
    /// cursors are kept apart from the buffers and more of them fit into the cache.
    struct Cursor
    {
        Key * pos = nullptr;
        Key * end = nullptr;
    };

    /// The size of the head block and of the buffer are up to date only after `syncBuffer`.
    struct Buffer
    {
        /// The most recent block, the only one that may be not full.
        Block * head = nullptr;
        size_t size = 0;
        /// The number of the oldest keys of the buffer that are known to be distinct: those that were left by its last
        /// deduplication, or moved from the local set.
        size_t num_distinct = 0;
    };

    struct Stream
    {
        /// Before the switch to the routing, the keys are inserted into this set. After the switch, it is reused
        /// as the scratch set for the deduplication of buffers.
        RadixSet<Key> local_set;
        Mode mode = Mode::BoundedSet;

        std::vector<Buffer> local_buffers;
        std::vector<Cursor> local_cursors;
        std::unique_ptr<Arena> arena = std::make_unique<Arena>(4096, 2, ARENA_LINEAR_GROWTH_THRESHOLD);
        std::array<Block *, MAX_BLOCK_CLASS + 1> free_blocks{};

        /// The total number of keys in the buffers, and the number at which they are deduplicated next (see
        /// `DEDUPLICATION_BUDGET_FACTOR`).
        size_t num_buffered = 0;
        size_t deduplication_budget = INITIAL_DEDUPLICATION_BUDGET;
        /// The buffer where the next sample for the deduplication starts.
        size_t next_sample_buffer = 0;
        /// Whether the empty string is routed already, so that it is not routed again: it is often the most frequent
        /// value of a `String` column, and it is not consecutive to itself when it alternates with other values.
        bool routed_empty_string = false;

        /// For the statistics: the number of keys passed to `insertBatch`, and the estimate of the number of distinct
        /// keys of the stream when it is finished.
        size_t num_routed = 0;
        size_t estimated_distinct = 0;
        /// For the statistics: whether the stream inserted its keys into a local set when it was finished.
        bool ended_in_set = false;
    };

    /// Starts the stream in the mode and with the budget that the statistics of a previous run tell (see the statistics in
    /// `RadixUniqExactTransform.h`).
    void startFromHint(Stream & stream)
    {
        const size_t stream_distinct = hint->stream_distinct;
        if (hint->streams_ended_in_set || stream_distinct * 2 * sizeof(Key) <= MAX_UNBOUNDED_SET_BYTES)
        {
            stream.mode = Mode::UnboundedSet;
            stream.local_set.reset(std::bit_ceil(std::max<size_t>(stream_distinct * 2, INITIAL_LOCAL_SET_CAPACITY)));
            return;
        }

        switchToRouting(stream);
        /// Mostly unique keys are not deduplicated until the stream grows to twice its previous size.
        if (hint->stream_keys <= stream_distinct * DEDUPLICATION_BUDGET_FACTOR * 2)
            stream.deduplication_budget = std::max(INITIAL_DEDUPLICATION_BUDGET, hint->stream_keys * 2);
        else
            stream.deduplication_budget = std::max(INITIAL_DEDUPLICATION_BUDGET, stream_distinct * DEDUPLICATION_BUDGET_FACTOR);
    }

    ALWAYS_INLINE void insert(Stream & stream, Key key)
    {
        if (stream.mode != Mode::Routing)
        {
            auto & set = stream.local_set;
            if (set.insert(key) && set.numCellsInUse() * 2 > set.capacity())
            {
                if (stream.mode == Mode::BoundedSet && set.capacity() * 2 * sizeof(Key) > MAX_LOCAL_SET_BYTES)
                    switchToRouting(stream);
                else
                    growLocalSet(set);
            }
            return;
        }

        push(stream, bucketHash(key) >> (64 - bucket_bits), key);
        if (++stream.num_buffered > stream.deduplication_budget) [[unlikely]]
            deduplicateStream(stream);
    }

    /// Inserts the keys into the local set or routes them, as `insert` does for each of them. The checks of `insert`
    /// are done once for as many keys as possible.
    ALWAYS_INLINE void insertBatch(Stream & stream, const Key * keys, size_t num_keys)
    {
        stream.num_routed += num_keys;
        size_t i = 0;
        while (i < num_keys)
        {
            if (stream.mode == Mode::Routing)
            {
                /// The keys within the budget are pushed without checking it, the next one starts a deduplication.
                const size_t num_pushed = std::min(stream.deduplication_budget - stream.num_buffered, num_keys - i);
                pushMany(stream, keys + i, num_pushed);
                i += num_pushed;
            }
            else
            {
                auto & set = stream.local_set;
                if ((set.numCellsInUse() + num_keys - i) * 2 <= set.capacity())
                {
                    set.insertMany(keys + i, num_keys - i);
                    return;
                }
            }

            /// The set may need to grow, the stream may switch to another mode, or the keys may be deduplicated.
            if (i < num_keys)
                insert(stream, keys[i++]);
        }
    }

    void pushMany(Stream & stream, const Key * keys, size_t num_keys)
    {
        const size_t shift = 64 - bucket_bits;
        for (size_t i = 0; i < num_keys; ++i)
            push(stream, bucketHash(keys[i]) >> shift, keys[i]);
        stream.num_buffered += num_keys;
    }

    static void growLocalSet(RadixSet<Key> & set)
    {
        RadixSet<Key> grown;
        grown.reset(set.capacity() * 2);
        set.forEach([&](Key key) { grown.insert(key); });
        set = std::move(grown);
    }

    void switchToRouting(Stream & stream)
    {
        stream.local_buffers.resize(num_buckets);
        stream.local_cursors.resize(num_buckets);
        stream.mode = Mode::Routing;
        stream.local_set.forEach([&](Key key) { push(stream, bucketHash(key) >> (64 - bucket_bits), key); });
        syncBuffers(stream);
        for (auto & buffer : stream.local_buffers)
            buffer.num_distinct = buffer.size;
        stream.num_buffered = stream.local_set.size();
        stream.deduplication_budget = std::max(INITIAL_DEDUPLICATION_BUDGET, stream.num_buffered * FIRST_DEDUPLICATION_BUDGET_FACTOR);
    }

    ALWAYS_INLINE static void push(Stream & stream, size_t bucket, Key key)
    {
        Cursor & cursor = stream.local_cursors[bucket];
        if (cursor.pos == cursor.end) [[unlikely]]
            addBlock(stream, bucket);
        *cursor.pos++ = key;
    }

    /// Brings the size of the head block and of the buffer up to date with its cursor.
    static void syncBuffer(Stream & stream, size_t bucket)
    {
        Buffer & buffer = stream.local_buffers[bucket];
        if (!buffer.head)
            return;
        const UInt32 head_size = static_cast<UInt32>(stream.local_cursors[bucket].pos - buffer.head->keys());
        buffer.size += head_size - buffer.head->size;
        buffer.head->size = head_size;
    }

    static void syncBuffers(Stream & stream)
    {
        for (size_t bucket = 0; bucket < stream.local_buffers.size(); ++bucket)
            syncBuffer(stream, bucket);
    }

    static void addBlock(Stream & stream, size_t bucket)
    {
        syncBuffer(stream, bucket);
        Buffer & buffer = stream.local_buffers[bucket];
        const UInt32 size_class = buffer.head ? std::min<UInt32>(buffer.head->size_class + 1, MAX_BLOCK_CLASS) : 0;

        Block * block = stream.free_blocks[size_class];
        if (block)
        {
            stream.free_blocks[size_class] = block->next;
        }
        else
        {
            block = reinterpret_cast<Block *>(
                stream.arena->alignedAlloc(sizeof(Block) + (MIN_BLOCK_SIZE << size_class) * sizeof(Key), alignof(Block)));
            block->size_class = size_class;
        }

        block->next = buffer.head;
        block->size = 0;
        buffer.head = block;
        stream.local_cursors[bucket] = {block->keys(), block->keys() + block->capacity()};
    }

    /// Deduplicates the buffered keys of the stream once they exceed the budget. The keys of a sample of buffers are
    /// deduplicated first. The buckets are selected by the hash, so the sample is representative: if it has few
    /// duplicates, the other buffers are left as is. Either way, the budget grows geometrically, so every buffered key
    /// takes part in a bounded number of deduplications on average, and the buffered keys stay within a small factor
    /// of the distinct keys of the stream.
    void deduplicateStream(Stream & stream)
    {
        /// The decisions are made by the medians over the sampled buffers, not by the totals: a key that is repeated
        /// much more often than the others (such as an empty string) is in a single buffer, and must not make the
        /// stream look like all its keys are repeated.
        std::vector<double> remaining_ratios;
        std::vector<double> new_distinct_ratios;
        size_t sampled_keys = 0;
        size_t num_deduplicated = 0;
        for (; num_deduplicated < num_buckets && (sampled_keys < DEDUPLICATION_SAMPLE_KEYS || num_deduplicated < DEDUPLICATION_SAMPLE_BUFFERS);
             ++num_deduplicated)
        {
            const size_t bucket = stream.next_sample_buffer;
            stream.next_sample_buffer = (stream.next_sample_buffer + 1) % num_buckets;
            syncBuffer(stream, bucket);
            auto & buffer = stream.local_buffers[bucket];
            const size_t before = buffer.size;
            const size_t known_distinct = buffer.num_distinct;
            deduplicateBuffer(stream, bucket);
            const size_t after = buffer.size;

            sampled_keys += before;
            stream.num_buffered -= before - after;
            if (before != 0)
                remaining_ratios.push_back(static_cast<double>(after) / static_cast<double>(before));
            /// Whether the keys are mostly repeated is decided by the keys that were routed since the last
            /// deduplication: once every key of the stream has been seen, the ratio of all the buffered keys to the
            /// distinct ones is `DEDUPLICATION_BUDGET_FACTOR` after every deduplication, whatever the number of repeats.
            if (before != known_distinct)
            {
                new_distinct_ratios.push_back(
                    static_cast<double>(after - known_distinct) / static_cast<double>(before - known_distinct));
            }
        }

        auto median = [](std::vector<double> & values, double if_empty)
        {
            if (values.empty())
                return if_empty;
            auto middle = values.begin() + values.size() / 2;
            std::nth_element(values.begin(), middle, values.end());
            return *middle;
        };
        const double remaining_ratio = median(remaining_ratios, 1);
        const double new_distinct_ratio = median(new_distinct_ratios, 1);

        const size_t estimated_size = static_cast<size_t>(static_cast<double>(stream.num_buffered) * remaining_ratio) + 1;
        if (new_distinct_ratio <= 0.25 && estimated_size * 2 * sizeof(Key) <= MAX_UNBOUNDED_SET_BYTES)
        {
            switchToUnboundedSet(stream, estimated_size);
            return;
        }

        if (remaining_ratio > 0.75)
        {
            stream.deduplication_budget = std::max(INITIAL_DEDUPLICATION_BUDGET, stream.num_buffered * DEDUPLICATION_BUDGET_FACTOR);
            return;
        }

        for (; num_deduplicated < num_buckets; ++num_deduplicated)
        {
            const size_t bucket = stream.next_sample_buffer;
            stream.next_sample_buffer = (stream.next_sample_buffer + 1) % num_buckets;
            syncBuffer(stream, bucket);
            auto & buffer = stream.local_buffers[bucket];
            stream.num_buffered -= buffer.size;
            deduplicateBuffer(stream, bucket);
            stream.num_buffered += buffer.size;
        }
        stream.deduplication_budget = std::max(INITIAL_DEDUPLICATION_BUDGET, stream.num_buffered * DEDUPLICATION_BUDGET_FACTOR);
    }

    /// Most of the keys of the stream are repeated, and their set is small, so they are better deduplicated right
    /// away, as the ordinary aggregation does: moves the buffered keys into the local set, which grows without a limit
    /// since then. The keys are moved to the buffers when the stream finishes.
    void switchToUnboundedSet(Stream & stream, size_t estimated_size)
    {
        auto & set = stream.local_set;
        set.reset(std::bit_ceil(std::max<size_t>(estimated_size * 2, INITIAL_LOCAL_SET_CAPACITY)));
        stream.mode = Mode::UnboundedSet;

        syncBuffers(stream);
        for (const auto & buffer : stream.local_buffers)
        {
            for (const Block * block = buffer.head; block; block = block->next)
            {
                for (size_t i = 0; i < block->size; ++i)
                    insert(stream, block->keys()[i]);
            }
        }

        stream.local_buffers.clear();
        stream.local_cursors.clear();
        stream.free_blocks = {};
        stream.arena = std::make_unique<Arena>(4096, 2, ARENA_LINEAR_GROWTH_THRESHOLD);
        stream.num_buffered = 0;
    }

    /// Deduplicates the keys of the buffer, whose size must be up to date (see `syncBuffer`).
    static void deduplicateBuffer(Stream & stream, size_t bucket)
    {
        Buffer & buffer = stream.local_buffers[bucket];
        if (buffer.size == 0)
            return;

        auto & set = stream.local_set;
        set.reset(std::bit_ceil(buffer.size * 2));

        for (const Block * block = buffer.head; block; block = block->next)
            set.insertMany(block->keys(), block->size);

        /// Return all the blocks to the free lists and push the distinct keys back, reusing the same blocks.
        for (Block * block = buffer.head; block;)
        {
            Block * next = block->next;
            block->next = stream.free_blocks[block->size_class];
            stream.free_blocks[block->size_class] = block;
            block = next;
        }

        buffer.head = nullptr;
        buffer.size = 0;
        stream.local_cursors[bucket] = {};
        set.forEach([&](Key key) { push(stream, bucket, key); });
        syncBuffer(stream, bucket);
        buffer.num_distinct = buffer.size;
    }

    const size_t bucket_bits;
    const size_t num_buckets;
    std::vector<Stream> streams;

    const StatsCollectingParams stats_collecting_params;
    const std::optional<RadixUniqExactEntry> hint;
    std::atomic<size_t> num_built_buckets{0};
};

/// The keys of up to 8 bytes are the values themselves, zero-extended to `UInt32` or `UInt64`. They are compared by
/// their bits, as `uniqExact` does.
template <typename Key, typename T>
void routeFixed(RadixUniqExactState<Key> & state, size_t stream, const IColumn & column, const UInt8 * null_map)
{
    state.template routeValues<T>(stream, column.getRawData().data(), column.size(), null_map); /// NOLINT(bugprone-suspicious-stringview-data-usage)
}

template <>
void RadixUniqExactState<UInt32>::route(size_t stream, const IColumn & column, const UInt8 * null_map)
{
    switch (column.sizeOfValueIfFixed())
    {
        case 1: routeFixed<UInt32, UInt8>(*this, stream, column, null_map); return;
        case 2: routeFixed<UInt32, UInt16>(*this, stream, column, null_map); return;
        case 4: routeFixed<UInt32, UInt32>(*this, stream, column, null_map); return;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in RadixUniqExactState", column.getName());
    }
}

template <>
void RadixUniqExactState<UInt64>::route(size_t stream, const IColumn & column, const UInt8 * null_map)
{
    if (column.sizeOfValueIfFixed() != sizeof(UInt64))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in RadixUniqExactState", column.getName());
    routeFixed<UInt64, UInt64>(*this, stream, column, null_map);
}

/// Strings and `IPv6` are represented by their 128-bit `SipHash`, the same key as in `uniqExact`, and other 16-byte
/// values by a bijection of themselves. Consecutive duplicates are compared by their bytes before they are hashed.
template <>
void RadixUniqExactState<UInt128>::route(size_t stream, const IColumn & column, const UInt8 * null_map)
{
    if (const auto * column_string = typeid_cast<const ColumnString *>(&column))
    {
        const char * chars = reinterpret_cast<const char *>(column_string->getChars().data());
        const auto * offsets = column_string->getOffsets().data();
        /// The offsets are padded on the left, and the offset before the first row is 0, as in `ColumnString::offsetAt`;
        /// the index is signed so that the row 0 does not wrap around.
        auto start_of = [offsets](size_t row) { return offsets[static_cast<ssize_t>(row) - 1]; };
        auto size_of = [offsets, start_of](size_t row) { return offsets[row] - start_of(row); };
        bool & routed_empty_string = streams[stream].routed_empty_string;
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row)
            {
                const size_t size = size_of(row);
                routed_empty_string |= size == 0;
                return sipHash128Padded(chars + start_of(row), size);
            },
            [&](size_t lhs, size_t rhs)
            {
                const size_t size = size_of(rhs);
                if (size == 0)
                    return routed_empty_string || size_of(lhs) == 0;
                return size == size_of(lhs) && memcmp(chars + start_of(lhs), chars + start_of(rhs), size) == 0;
            });
    }
    else if (const auto * column_fixed_string = typeid_cast<const ColumnFixedString *>(&column))
    {
        const char * chars = reinterpret_cast<const char *>(column_fixed_string->getChars().data());
        const size_t n = column_fixed_string->getN();
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row) { return sipHash128Padded(chars + row * n, n); },
            [&](size_t lhs, size_t rhs) { return memcmp(chars + lhs * n, chars + rhs * n, n) == 0; });
    }
    else if (const auto * column_ipv6 = typeid_cast<const ColumnIPv6 *>(&column))
    {
        const auto & data = column_ipv6->getData();
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row) { return sipHash128Padded(reinterpret_cast<const char *>(&data[row]), sizeof(IPv6)); },
            [&](size_t lhs, size_t rhs) { return data[lhs] == data[rhs]; });
    }
    else if (column.isFixedAndContiguous() && column.sizeOfValueIfFixed() == sizeof(UInt128))
    {
        const char * bytes = column.getRawData().data();
        auto data = [bytes](size_t row) { return unalignedLoad<UInt128>(bytes + row * sizeof(UInt128)); };
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row) { return mixKey(data(row)); },
            [&](size_t lhs, size_t rhs) { return data(lhs) == data(rhs); });
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in RadixUniqExactState", column.getName());
    }
}

/// With the statistics, there are enough buckets for each bucket set to fit into half of the L2 cache.
size_t bucketBits(size_t num_streams, size_t key_size, const std::optional<RadixUniqExactEntry> & hint)
{
    size_t num_buckets = num_streams * BUCKETS_PER_STREAM;
    if (hint)
        num_buckets = std::max(num_buckets, hint->num_distinct * 2 * key_size / std::max<size_t>(getL2CacheSize() / 2, 1));
    return std::clamp<size_t>(std::bit_width(num_buckets - 1), MIN_BUCKET_BITS, MAX_BUCKET_BITS);
}

template <typename Key>
RadixUniqExactStatePtr createState(size_t num_streams, const StatsCollectingParams & stats_collecting_params)
{
    std::optional<RadixUniqExactEntry> hint;
    if (stats_collecting_params.isCollectionAndUseEnabled())
    {
        hint = getHashTablesStatistics<RadixUniqExactEntry>().getSizeHint(stats_collecting_params);
        /// The numbers of a stream are not comparable with a different number of streams.
        if (hint && hint->num_streams != num_streams)
            hint.reset();

        /// As the ordinary aggregation, do not preallocate the sets above `max_size_to_preallocate`: one local set per
        /// stream, then the bucket sets.
        const size_t preallocated = hint ? std::max(hint->num_distinct, hint->stream_distinct * num_streams) : 0;
        if (preallocated > stats_collecting_params.max_size_to_preallocate)
        {
            LOG_TRACE(
                getLogger("HashTablesStatistics"),
                "No space were preallocated in hash tables because 'max_size_to_preallocate' has too small value: {}, should be at "
                "least {}",
                stats_collecting_params.max_size_to_preallocate,
                preallocated);
            hint.reset();
        }
    }

    return std::make_shared<RadixUniqExactState<Key>>(
        num_streams, bucketBits(num_streams, sizeof(Key), hint), stats_collecting_params, hint);
}

}

namespace
{

/// The types for which `uniqExact` keeps the value itself in the set, and their size if it is at most 64 bits.
std::optional<size_t> valueKeySize(const IDataType & type)
{
    WhichDataType which(type);
    if (which.isNativeInteger() || which.isFloat() || which.isEnum() || which.isDate() || which.isDate32() || which.isDateTime() || which.isIPv4())
        return type.getSizeOfValueInMemory();
    return {};
}

bool isHashedKey(const IDataType & type)
{
    WhichDataType which(type);
    return which.isUInt128() || which.isInt128() || which.isUUID() || which.isString() || which.isFixedString() || which.isIPv6();
}

}

bool isRadixUniqExactSupported(const IDataType & argument_type)
{
    const IDataType & type = *removeNullable(argument_type.shared_from_this());
    if (const auto size = valueKeySize(type))
        return *size <= sizeof(UInt64);
    return isHashedKey(type);
}

RadixUniqExactStatePtr createRadixUniqExactState(
    const IDataType & argument_type, size_t num_streams, const StatsCollectingParams & stats_collecting_params)
{
    const IDataType & type = *removeNullable(argument_type.shared_from_this());
    if (const auto size = valueKeySize(type))
    {
        if (*size <= sizeof(UInt32))
            return createState<UInt32>(num_streams, stats_collecting_params);
        if (*size == sizeof(UInt64))
            return createState<UInt64>(num_streams, stats_collecting_params);
    }
    else if (isHashedKey(type))
        return createState<UInt128>(num_streams, stats_collecting_params);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported argument type {} for the radix partitioned uniqExact", argument_type.getName());
}


bool IRadixUniqExactState::useOwnTracker() const
{
    return memory_tracker && CurrentThread::getMemoryTracker()
        && CurrentThread::getMemoryTracker()->getParent() == memory_tracker->getParent();
}

std::optional<UInt64> IRadixUniqExactState::getPeakMemoryUsage() const
{
    if (!memory_tracker)
        return std::nullopt;
    return std::max<Int64>(memory_tracker->getPeak(), 0);
}


RadixUniqExactRouteTransform::RadixUniqExactRouteTransform(
    SharedHeader input_header, SharedHeader output_header, RadixUniqExactStatePtr state_, size_t stream_, size_t key_position_)
    : IProcessor(InputPorts{input_header}, OutputPorts{output_header})
    , state(std::move(state_))
    , stream(stream_)
    , key_position(key_position_)
{
}

IProcessor::Status RadixUniqExactRouteTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (has_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        if (!stream_finished)
            return Status::Ready;

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull();
    has_chunk = true;
    return Status::Ready;
}

void RadixUniqExactRouteTransform::work()
{
    if (!has_chunk)
    {
        RADIX_UNIQ_EXACT_TRACK_MEMORY(state);
        state->finishStream(stream);
        stream_finished = true;
        return;
    }

    if (rows_before_aggregation)
        rows_before_aggregation->add(current_chunk.getNumRows());

    ColumnPtr column = current_chunk.getColumns()[key_position];
    /// A constant is routed once. An empty chunk of it routes nothing, so that the rows stay at zero.
    if (const auto * column_const = typeid_cast<const ColumnConst *>(column.get()))
        column = column_const->empty() ? column_const->getDataColumnPtr()->cloneEmpty() : column_const->getDataColumnPtr();
    /// Only the values of a sparse column are routed: the default value is at position 0, followed by the other ones.
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(column.get()))
    {
        const auto & values = column_sparse->getValuesPtr();
        column = column_sparse->getNumberOfDefaultRows() ? values : values->cut(1, values->size() - 1);
    }
    column = column->convertToFullIfWrapped();
    {
        RADIX_UNIQ_EXACT_TRACK_MEMORY(state);
        if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
            state->route(stream, column_nullable->getNestedColumn(), column_nullable->getNullMapData().data());
        else
            state->route(stream, *column, nullptr);
    }

    current_chunk.clear();
    has_chunk = false;
}


RadixUniqExactBarrierTransform::RadixUniqExactBarrierTransform(SharedHeader header, size_t num_inputs, size_t num_outputs)
    : IProcessor(InputPorts(num_inputs, header), OutputPorts(num_outputs, header))
{
}

IProcessor::Status RadixUniqExactBarrierTransform::prepare()
{
    bool all_outputs_finished = true;
    for (const auto & output : outputs)
        all_outputs_finished &= output.isFinished();

    if (all_outputs_finished)
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    bool all_inputs_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;

        /// The route transforms never push data, they only finish.
        all_inputs_finished = false;
        input.setNeeded();
        if (input.hasData())
            input.pull();
    }

    if (!all_inputs_finished)
        return Status::NeedData;

    for (auto & output : outputs)
        output.finish();
    return Status::Finished;
}


RadixUniqExactBuildTransform::RadixUniqExactBuildTransform(SharedHeader header, RadixUniqExactStatePtr state_)
    : IProcessor(InputPorts{header}, OutputPorts{header})
    , state(std::move(state_))
{
}

IProcessor::Status RadixUniqExactBuildTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (built)
    {
        if (!output.canPush())
            return Status::PortFull;

        if (result)
        {
            output.push(std::move(result));
            return Status::PortFull;
        }

        output.finish();
        return Status::Finished;
    }

    /// The input finishes once all the streams are routed.
    if (!input.isFinished())
    {
        input.setNeeded();
        if (input.hasData())
            input.pull();
        return Status::NeedData;
    }

    return Status::Ready;
}

void RadixUniqExactBuildTransform::work()
{
    built = true;

    UInt64 count = 0;
    {
        RADIX_UNIQ_EXACT_TRACK_MEMORY(state);
        for (size_t bucket = state->claimBucket(); bucket < state->numBuckets(); bucket = state->claimBucket())
        {
            /// A partial count must not reach the result.
            if (isCancelled())
                return;
            count += state->buildBucket(bucket);
        }
    }

    auto column = ColumnUInt64::create();
    column->insertValue(count);
    result = Chunk(Columns{std::move(column)}, 1);
}


RadixUniqExactSumTransform::RadixUniqExactSumTransform(SharedHeader header, RadixUniqExactStatePtr state_, bool empty_result_for_empty_set_)
    : IAccumulatingTransform(header, header)
    , state(std::move(state_))
    , empty_result_for_empty_set(empty_result_for_empty_set_)
{
}

void RadixUniqExactSumTransform::consume(Chunk chunk)
{
    const auto & column = assert_cast<const ColumnUInt64 &>(*chunk.getColumns().front());
    for (UInt64 value : column.getData())
        total += value;
}

Chunk RadixUniqExactSumTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    state->recordStatistics(total);

    /// Like the aggregation without keys, return a single row even for an empty input, unless asked otherwise.
    if (empty_result_for_empty_set && state->numRows() == 0)
        return {};

    auto column = ColumnUInt64::create();
    column->insertValue(total);
    return Chunk(Columns{std::move(column)}, 1);
}

}
