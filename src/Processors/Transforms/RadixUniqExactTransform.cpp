#include <Processors/Transforms/RadixUniqExactTransform.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <base/getL2CacheSize.h>
#include <Interpreters/HashTablesStatistics.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

#include <array>
#include <bit>
#include <cstring>
#include <limits>
#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The partition of a key is selected by the top bits of `intHash64`, the finalizer of MurmurHash3:
/// - Every bit of the key affects every bit of the hash, so the keys that differ only in their low bits, such as
///   sequential identifiers or timestamps, are spread evenly over the partitions. The top bits of the key itself, or of
///   a weak hash, would send them all to a few partitions, and the build of these partitions would not be balanced.
/// - `intHashCRC32` is not good enough for it: it has only 32 bits, and they are not well mixed (see its comment).
/// - It is cheap (two multiplications) and computed only once per routed key.
ALWAYS_INLINE inline UInt64 partitionHash(UInt64 x)
{
    return intHash64(x);
}

ALWAYS_INLINE inline UInt64 partitionHash(UInt32 x)
{
    return intHash64(x);
}

/// The 128-bit keys are uniformly distributed (see `mixKey`), so their low half is used as the hash, and the high half
/// is used for the slot.
ALWAYS_INLINE inline UInt64 partitionHash(UInt128 x)
{
    return static_cast<UInt64>(x);
}

/// The slot of a key in a set is selected by the top bits of a different hash:
/// - The keys of a partition share the top bits of `partitionHash`, so a hash that is independent of it is needed to
///   spread them over the whole set.
/// - It is computed for every insertion into a set (the local set, the deduplication of the buffered keys, the build
///   of a partition), so it must be as cheap as possible. It is Fibonacci hashing (Knuth, TAOCP vol. 3, 6.4): the
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
    UInt64 high = static_cast<UInt64>(x >> 64) ^ partitionHash(low);
    low ^= partitionHash(high);
    return (UInt128(high) << 64) | low;
}

/// When a set does not fit into the L2 cache, the cell of the key this far ahead is prefetched while a key is inserted,
/// so that the cache misses of the inserts overlap.
constexpr size_t PREFETCH_DISTANCE = 32;

/// Open addressing set with linear probing. The zero key is kept out of the cells, because zero marks an empty cell.
template <typename Key>
class FlatSet
{
public:
    /// Empties the set and makes its capacity `capacity`, a power of two.
    void reset(size_t capacity)
    {
        chassert(std::has_single_bit(capacity));
        cells.resize(capacity);
        memset(cells.data(), 0, capacity * sizeof(Key));
        mask = capacity - 1;
        shift = 64 - std::countr_zero(capacity);
        count = 0;
        has_zero = false;
    }

    /// Returns true if the key was not in the set.
    ALWAYS_INLINE bool insert(Key key)
    {
        if (key == Key{})
        {
            const bool inserted = !has_zero;
            has_zero = true;
            return inserted;
        }

        size_t place = slotHash(key) >> shift;
        while (true)
        {
            Key & cell = cells[place];
            if (cell == key)
                return false;
            if (cell == Key{})
            {
                cell = key;
                ++count;
                return true;
            }
            place = (place + 1) & mask;
        }
    }

    /// Inserts the keys, which must fit without exceeding the load factor of 0.5. The fields are kept in local
    /// variables, because the stores into the cells could alias them.
    template <bool prefetch>
    void insertMany(const Key * __restrict keys, size_t num_keys)
    {
        chassert((count + num_keys) * 2 <= capacity());
        Key * __restrict data = cells.data();
        const size_t local_mask = mask;
        const size_t local_shift = shift;
        size_t local_count = count;
        bool local_has_zero = has_zero;

        for (size_t i = 0; i < num_keys; ++i)
        {
            if (prefetch && i + PREFETCH_DISTANCE < num_keys)
                __builtin_prefetch(&data[slotHash(keys[i + PREFETCH_DISTANCE]) >> local_shift]);

            const Key key = keys[i];
            if (key == Key{})
            {
                local_has_zero = true;
                continue;
            }

            size_t place = slotHash(key) >> local_shift;
            while (true)
            {
                const Key cell = data[place];
                if (cell == key)
                    break;
                if (cell == Key{})
                {
                    data[place] = key;
                    ++local_count;
                    break;
                }
                place = (place + 1) & local_mask;
            }
        }

        count = local_count;
        has_zero = local_has_zero;
    }

    /// Inserts the keys, choosing whether to prefetch by the size of the set.
    void insertMany(const Key * keys, size_t num_keys)
    {
        if (capacity() * sizeof(Key) > getL2CacheSize())
            insertMany<true>(keys, num_keys);
        else
            insertMany<false>(keys, num_keys);
    }

    size_t size() const { return count + has_zero; }
    size_t capacity() const { return mask + 1; }
    size_t numCellsInUse() const { return count; }

    template <typename Func>
    void forEach(Func && func) const
    {
        if (has_zero)
            func(Key{});
        for (size_t i = 0; i <= mask; ++i)
            if (cells[i] != Key{})
                func(cells[i]);
    }

private:
    PODArray<Key> cells;
    size_t mask = 0;
    size_t shift = 64;
    size_t count = 0;
    bool has_zero = false;
};

/// A stream switches from its local set to the routing once the set would outgrow this size.
constexpr size_t MAX_LOCAL_SET_BYTES = 1024 * 1024;

/// A stream deduplicates its buffered keys once it has buffered this many keys for the first time.
constexpr size_t INITIAL_COMPACTION_BUDGET = 64 * 1024;

/// The number of buffered keys of a stream, and the number of chains, that are deduplicated first to estimate whether
/// the deduplication of all of them is worth it.
constexpr size_t COMPACTION_SAMPLE_KEYS = 4096;
constexpr size_t COMPACTION_SAMPLE_CHAINS = 8;

/// After a deduplication, the budget is this many times the number of keys that remain buffered. Each deduplication
/// inserts all the buffered keys again, so a large factor makes the repeated keys cheaper, at the cost of memory.
constexpr size_t COMPACTION_BUDGET_FACTOR = 8;

/// The first deduplication after the switch to the routing comes sooner, because it decides whether the stream goes
/// back to its local set, and the keys routed until then are wasted work if it does.
constexpr size_t FIRST_COMPACTION_BUDGET_FACTOR = 4;

/// A stream whose keys are mostly repeated goes back to inserting them into its local set only if the set would not
/// be larger than this. Larger sets do not fit into the CPU caches, and the inserts into them are slower than routing
/// and deduplicating the keys in cache-resident sets, even if every key is deduplicated several times.
constexpr size_t MAX_UNBOUNDED_SET_BYTES = 4 * 1024 * 1024;

/// The number of partitions per stream. Many partitions keep the partition sets cache-resident even when there are
/// many distinct keys, and they balance the build between the threads.
constexpr size_t PARTITIONS_PER_STREAM = 16;

/// At least 256 partitions, and at most 4096, because every stream keeps a chain per partition.
constexpr size_t MIN_PARTITION_BITS = 8;
constexpr size_t MAX_PARTITION_BITS = 12;

/// The blocks of a chain of buffered keys grow from `MIN_BLOCK_SIZE` to `MIN_BLOCK_SIZE << MAX_BLOCK_CLASS` keys,
/// so that the chains of the partitions with few keys stay small.
constexpr size_t MIN_BLOCK_SIZE = 16;
constexpr size_t MAX_BLOCK_CLASS = 6;

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
        size_t num_streams_, size_t partition_bits_, const StatsCollectingParams & stats_collecting_params_, std::optional<RadixUniqExactEntry> hint_)
        : partition_bits(partition_bits_)
        , num_partitions(1ULL << partition_bits_)
        , streams(num_streams_)
        , stats_collecting_params(stats_collecting_params_)
        , hint(hint_)
    {
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
    template <typename T>
    void routeValues(size_t stream_index, const T * data, size_t num_rows, const UInt8 * null_map)
    {
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
                    const T value = data[row];
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
                            all_same &= data[block + i] == data[block + i - 1];
                        if (all_same)
                            continue;
                    }

                    for (size_t row = block; row < block_end; ++row)
                    {
                        keys[num_keys] = static_cast<Key>(data[row]);
                        num_keys += row == 0 || data[row] != data[row - 1];
                    }
                }
            }
            else
            {
                for (size_t row = begin; row < end; ++row)
                    keys[num_keys++] = static_cast<Key>(data[row]);
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
        /// The local set holds distinct keys, so no chain needs a deduplication while it is moved.
        if (stream.mode != Mode::Radix)
        {
            stream.estimated_distinct = stream.local_set.size();
            switchToRadixMode(stream);
        }
        else
        {
            stream.estimated_distinct = static_cast<size_t>(static_cast<double>(stream.num_buffered) * stream.remaining_ratio);
        }

        /// The build reads only the chains, so the set must not hold memory while the partition sets are built.
        stream.local_set = {};
    }

    size_t buildPartition(size_t partition) override
    {
        size_t total = 0;
        for (const auto & stream : streams)
        {
            if (stream.mode != Mode::Radix)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A partition is built before all the streams are routed");
            total += stream.chains[partition].size;
        }

        /// Counted before the early return of an empty partition, the statistics need every partition to be built.
        num_built_partitions.fetch_add(1, std::memory_order_relaxed);
        if (total == 0)
            return 0;

        /// The load factor is at most 0.5, the actual number of distinct keys can only be lower. With the statistics, the
        /// set is allocated for the expected number of distinct keys of a partition, with a margin for the variation
        /// between the partitions, and it grows if there are more of them.
        size_t expected = total;
        if (hint)
        {
            const size_t partition_distinct = hint->num_distinct / num_partitions;
            expected = std::min(total, partition_distinct + partition_distinct / 16 + MIN_BLOCK_SIZE);
        }

        FlatSet<Key> set;
        set.reset(std::bit_ceil(expected * 2));

        for (const auto & stream : streams)
        {
            for (const Block * block = stream.chains[partition].head; block; block = block->next)
            {
                while ((set.numCellsInUse() + block->size) * 2 > set.capacity())
                    growLocalSet(set);
                set.insertMany(block->keys(), block->size);
            }
        }

        return set.size();
    }

    void recordStatistics(size_t num_distinct) override
    {
        if (!stats_collecting_params.isCollectionAndUseEnabled() || num_built_partitions.load(std::memory_order_relaxed) != num_partitions)
            return;

        std::vector<size_t> stream_distinct;
        std::vector<size_t> stream_keys;
        for (const auto & stream : streams)
        {
            stream_distinct.push_back(stream.estimated_distinct);
            stream_keys.push_back(stream.num_routed);
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
                .num_streams = streams.size()},
            stats_collecting_params);
    }

    size_t numPartitions() const override { return num_partitions; }

private:
    static constexpr size_t INITIAL_LOCAL_SET_CAPACITY = 256;

    /// The keys of a column are computed into a buffer of this size, which is then inserted.
    static constexpr size_t KEYS_BATCH_SIZE = 256;

    enum class Mode : uint8_t
    {
        /// The keys are inserted into the local set, until it outgrows the cache.
        LocalSet,
        /// The keys are buffered in the chains of the partitions.
        Radix,
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

    struct Chain
    {
        /// The most recent block, the only one that may be not full.
        Block * head = nullptr;
        size_t size = 0;
        /// The number of the oldest keys of the chain that are known to be distinct: those that were left by its last
        /// deduplication, or moved from the local set.
        size_t num_distinct = 0;
    };

    struct Stream
    {
        /// Before the switch to the radix mode, the keys are inserted into this set. After the switch, it is reused
        /// as the scratch set for the deduplication of chains.
        FlatSet<Key> local_set;
        Mode mode = Mode::LocalSet;

        std::vector<Chain> chains;
        std::unique_ptr<Arena> arena = std::make_unique<Arena>();
        std::array<Block *, MAX_BLOCK_CLASS + 1> free_blocks{};

        /// The total number of keys in the chains, and the number at which they are deduplicated.
        size_t num_buffered = 0;
        size_t compaction_budget = INITIAL_COMPACTION_BUDGET;
        /// The chain where the next sample for the deduplication starts.
        size_t next_sample_chain = 0;
        /// Whether the empty string is routed already, so that it is not routed again: it is often the most frequent
        /// value of a `String` column, and it is not consecutive to itself when it alternates with other values.
        bool routed_empty_string = false;
        /// The ratio of the keys that remained after the last deduplication in its sample.
        double remaining_ratio = 1;

        /// For the statistics: the number of keys passed to `insertBatch`, and the estimate of the number of distinct
        /// keys of the stream when it is finished.
        size_t num_routed = 0;
        size_t estimated_distinct = 0;
    };

    /// The statistics of a previous run of the query tell the number of distinct keys of a stream, so the stream
    /// starts in the mode that suits it: in a local set allocated for all its keys if it fits into the CPU caches, and
    /// in the radix mode otherwise, without deduplications if its keys are mostly distinct.
    void startFromHint(Stream & stream)
    {
        const size_t stream_distinct = hint->stream_distinct;
        if (stream_distinct * 2 * sizeof(Key) <= MAX_UNBOUNDED_SET_BYTES)
        {
            stream.mode = Mode::UnboundedSet;
            stream.local_set.reset(std::bit_ceil(std::max<size_t>(stream_distinct * 2, INITIAL_LOCAL_SET_CAPACITY)));
            return;
        }

        switchToRadixMode(stream);
        if (hint->stream_keys * 3 <= stream_distinct * 4)
            stream.compaction_budget = std::numeric_limits<size_t>::max() / 2;
        else
            stream.compaction_budget = std::max(INITIAL_COMPACTION_BUDGET, stream_distinct * COMPACTION_BUDGET_FACTOR);
    }

    ALWAYS_INLINE void insert(Stream & stream, Key key)
    {
        if (stream.mode != Mode::Radix)
        {
            auto & set = stream.local_set;
            if (set.insert(key) && set.numCellsInUse() * 2 > set.capacity())
            {
                if (stream.mode == Mode::LocalSet && set.capacity() * 2 * sizeof(Key) > MAX_LOCAL_SET_BYTES)
                    switchToRadixMode(stream);
                else
                    growLocalSet(set);
            }
            return;
        }

        push(stream, stream.chains[partitionHash(key) >> (64 - partition_bits)], key);
        if (++stream.num_buffered > stream.compaction_budget) [[unlikely]]
            compactStream(stream);
    }

    /// Inserts the keys into the local set or routes them, as `insert` does for each of them. The checks of `insert`
    /// are done once for as many keys as possible.
    ALWAYS_INLINE void insertBatch(Stream & stream, const Key * keys, size_t num_keys)
    {
        stream.num_routed += num_keys;
        size_t i = 0;
        while (i < num_keys)
        {
            if (stream.mode == Mode::Radix)
            {
                /// The keys within the budget are pushed without checking it, the next one starts a deduplication.
                const size_t num_pushed = std::min(stream.compaction_budget - stream.num_buffered, num_keys - i);
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
        Chain * chains = stream.chains.data();
        const size_t shift = 64 - partition_bits;
        for (size_t i = 0; i < num_keys; ++i)
            push(stream, chains[partitionHash(keys[i]) >> shift], keys[i]);
        stream.num_buffered += num_keys;
    }

    static void growLocalSet(FlatSet<Key> & set)
    {
        FlatSet<Key> grown;
        grown.reset(set.capacity() * 2);
        set.forEach([&](Key key) { grown.insert(key); });
        set = std::move(grown);
    }

    void switchToRadixMode(Stream & stream)
    {
        stream.chains.resize(num_partitions);
        stream.mode = Mode::Radix;
        stream.local_set.forEach([&](Key key) { push(stream, stream.chains[partitionHash(key) >> (64 - partition_bits)], key); });
        for (auto & chain : stream.chains)
            chain.num_distinct = chain.size;
        stream.num_buffered = stream.local_set.size();
        stream.compaction_budget = std::max(INITIAL_COMPACTION_BUDGET, stream.num_buffered * FIRST_COMPACTION_BUDGET_FACTOR);
    }

    ALWAYS_INLINE static void push(Stream & stream, Chain & chain, Key key)
    {
        if (!chain.head || chain.head->size == chain.head->capacity()) [[unlikely]]
            addBlock(stream, chain);

        chain.head->keys()[chain.head->size++] = key;
        ++chain.size;
    }

    static void addBlock(Stream & stream, Chain & chain)
    {
        const UInt32 size_class = chain.head ? std::min<UInt32>(chain.head->size_class + 1, MAX_BLOCK_CLASS) : 0;

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

        block->next = chain.head;
        block->size = 0;
        chain.head = block;
    }

    /// Deduplicates the buffered keys of the stream once they exceed the budget. The keys of a sample of chains are
    /// deduplicated first. The partitions are selected by the hash, so the sample is representative: if it has few
    /// duplicates, the other chains are left as is. Either way, the budget grows geometrically, so every buffered key
    /// takes part in a bounded number of deduplications on average, and the buffered keys stay within a small factor
    /// of the distinct keys of the stream.
    void compactStream(Stream & stream)
    {
        /// The decisions are made by the medians over the sampled chains, not by the totals: a key that is repeated
        /// much more often than the others (such as an empty string) is in a single chain, and must not make the
        /// stream look like all its keys are repeated.
        std::vector<double> remaining_ratios;
        std::vector<double> new_distinct_ratios;
        size_t sampled_keys = 0;
        size_t num_compacted = 0;
        for (; num_compacted < num_partitions && (sampled_keys < COMPACTION_SAMPLE_KEYS || num_compacted < COMPACTION_SAMPLE_CHAINS);
             ++num_compacted)
        {
            auto & chain = stream.chains[stream.next_sample_chain];
            stream.next_sample_chain = (stream.next_sample_chain + 1) % num_partitions;
            const size_t before = chain.size;
            const size_t known_distinct = chain.num_distinct;
            compact(stream, chain);
            const size_t after = chain.size;

            sampled_keys += before;
            stream.num_buffered -= before - after;
            if (before != 0)
                remaining_ratios.push_back(static_cast<double>(after) / static_cast<double>(before));
            /// Whether the keys are mostly repeated is decided by the keys that were routed since the last
            /// deduplication: once every key of the stream has been seen, the ratio of all the buffered keys to the
            /// distinct ones is `COMPACTION_BUDGET_FACTOR` after every deduplication, whatever the number of repeats.
            if (before != known_distinct)
                new_distinct_ratios.push_back(
                    static_cast<double>(after - known_distinct) / static_cast<double>(before - known_distinct));
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
        stream.remaining_ratio = remaining_ratio;
        const double new_distinct_ratio = median(new_distinct_ratios, 1);

        const size_t estimated_size = static_cast<size_t>(static_cast<double>(stream.num_buffered) * remaining_ratio) + 1;
        if (new_distinct_ratio <= 0.25 && estimated_size * 2 * sizeof(Key) <= MAX_UNBOUNDED_SET_BYTES)
        {
            switchToUnboundedSetMode(stream, estimated_size);
            return;
        }

        if (remaining_ratio > 0.75)
        {
            stream.compaction_budget = std::max(INITIAL_COMPACTION_BUDGET, stream.num_buffered * COMPACTION_BUDGET_FACTOR);
            return;
        }

        for (; num_compacted < num_partitions; ++num_compacted)
        {
            auto & chain = stream.chains[stream.next_sample_chain];
            stream.next_sample_chain = (stream.next_sample_chain + 1) % num_partitions;
            stream.num_buffered -= chain.size;
            compact(stream, chain);
            stream.num_buffered += chain.size;
        }
        stream.compaction_budget = std::max(INITIAL_COMPACTION_BUDGET, stream.num_buffered * COMPACTION_BUDGET_FACTOR);
    }

    /// Most of the keys of the stream are repeated, and their set is small, so they are better deduplicated right
    /// away, as the ordinary aggregation does: moves the buffered keys into the local set, which grows without a limit
    /// since then. The keys are moved to the chains when the stream finishes.
    void switchToUnboundedSetMode(Stream & stream, size_t estimated_size)
    {
        auto & set = stream.local_set;
        set.reset(std::bit_ceil(std::max<size_t>(estimated_size * 2, INITIAL_LOCAL_SET_CAPACITY)));
        stream.mode = Mode::UnboundedSet;

        for (const auto & chain : stream.chains)
            for (const Block * block = chain.head; block; block = block->next)
                for (size_t i = 0; i < block->size; ++i)
                    insert(stream, block->keys()[i]);

        stream.chains.clear();
        stream.free_blocks = {};
        stream.arena = std::make_unique<Arena>();
        stream.num_buffered = 0;
    }

    /// Deduplicates the keys of the chain.
    static void compact(Stream & stream, Chain & chain)
    {
        if (chain.size == 0)
            return;

        auto & set = stream.local_set;
        set.reset(std::bit_ceil(chain.size * 2));

        for (const Block * block = chain.head; block; block = block->next)
            set.insertMany(block->keys(), block->size);

        /// Return all the blocks to the free lists and push the distinct keys back, reusing the same blocks.
        for (Block * block = chain.head; block;)
        {
            Block * next = block->next;
            block->next = stream.free_blocks[block->size_class];
            stream.free_blocks[block->size_class] = block;
            block = next;
        }

        chain.head = nullptr;
        chain.size = 0;
        set.forEach([&](Key key) { push(stream, chain, key); });
        chain.num_distinct = chain.size;
    }

    const size_t partition_bits;
    const size_t num_partitions;
    std::vector<Stream> streams;

    const StatsCollectingParams stats_collecting_params;
    const std::optional<RadixUniqExactEntry> hint;
    std::atomic<size_t> num_built_partitions{0};
};

/// The keys of up to 8 bytes are the values themselves, zero-extended to `UInt32` or `UInt64`. They are compared by
/// their bits, as `uniqExact` does.
template <typename Key, typename T>
void routeFixed(RadixUniqExactState<Key> & state, size_t stream, const IColumn & column, const UInt8 * null_map)
{
    state.routeValues(stream, reinterpret_cast<const T *>(column.getRawData().data()), column.size(), null_map);
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
        const auto * data = reinterpret_cast<const UInt128 *>(column.getRawData().data());
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row) { return mixKey(data[row]); },
            [&](size_t lhs, size_t rhs) { return data[lhs] == data[rhs]; });
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} in RadixUniqExactState", column.getName());
    }
}

/// With the statistics, there are enough partitions for each partition set to fit into half of the L2 cache.
size_t partitionBits(size_t num_streams, size_t key_size, const std::optional<RadixUniqExactEntry> & hint)
{
    size_t num_partitions = num_streams * PARTITIONS_PER_STREAM;
    if (hint)
        num_partitions = std::max(num_partitions, hint->num_distinct * 2 * key_size / std::max<size_t>(getL2CacheSize() / 2, 1));
    return std::clamp<size_t>(std::bit_width(num_partitions - 1), MIN_PARTITION_BITS, MAX_PARTITION_BITS);
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
    }

    return std::make_shared<RadixUniqExactState<Key>>(
        num_streams, partitionBits(num_streams, sizeof(Key), hint), stats_collecting_params, hint);
}

}

RadixUniqExactStatePtr createRadixUniqExactState(
    const IDataType & argument_type, size_t num_streams, const StatsCollectingParams & stats_collecting_params)
{
    const IDataType & type = *removeNullable(argument_type.shared_from_this());
    WhichDataType which(type);

    /// The types for which `uniqExact` keeps the value itself in the set.
    if (which.isNativeInteger() || which.isFloat() || which.isEnum() || which.isDate() || which.isDate32() || which.isDateTime() || which.isIPv4())
    {
        const size_t size = type.getSizeOfValueInMemory();
        if (size <= sizeof(UInt32))
            return createState<UInt32>(num_streams, stats_collecting_params);
        if (size == sizeof(UInt64))
            return createState<UInt64>(num_streams, stats_collecting_params);
        return nullptr;
    }

    if (which.isUInt128() || which.isInt128() || which.isUUID() || which.isString() || which.isFixedString() || which.isIPv6())
        return createState<UInt128>(num_streams, stats_collecting_params);

    return nullptr;
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
        state->finishStream(stream);
        stream_finished = true;
        return;
    }

    ColumnPtr column = current_chunk.getColumns()[key_position];
    /// Only the values of a sparse column are routed: the default value is at position 0, followed by the other ones.
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(column.get()))
    {
        const auto & values = column_sparse->getValuesPtr();
        column = column_sparse->getNumberOfDefaultRows() ? values : values->cut(1, values->size() - 1);
    }
    column = column->convertToFullIfWrapped();
    if (const auto * column_nullable = typeid_cast<const ColumnNullable *>(column.get()))
        state->route(stream, column_nullable->getNestedColumn(), column_nullable->getNullMapData().data());
    else
        state->route(stream, *column, nullptr);

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
    for (size_t partition = state->claimPartition(); partition < state->numPartitions(); partition = state->claimPartition())
    {
        /// A partial count must not reach the result.
        if (isCancelled())
            return;
        count += state->buildPartition(partition);
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
