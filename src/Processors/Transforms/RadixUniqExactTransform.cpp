#include <Processors/Transforms/RadixUniqExactTransform.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/HashTable/Hash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

#include <array>
#include <bit>
#include <cstring>


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

/// The 128-bit keys are uniformly distributed (see `mixKey`), so their low half is used as the hash.
ALWAYS_INLINE inline UInt64 partitionHash(UInt128 x)
{
    return static_cast<UInt64>(x);
}

/// The slot of a key in a set is selected by a different hash:
/// - The keys of a partition share the top bits of `partitionHash`, so a hash that is independent of it is needed to
///   spread them over the whole set.
/// - It is computed for every insertion into a set (the local set, the deduplication of the buffered keys, the build
///   of a partition), so it must be as cheap as possible. CRC32 is a single instruction, and the set uses only its low
///   bits, which are mixed well enough for that, as in the ordinary `uniqExact`.
ALWAYS_INLINE inline UInt64 slotHash(UInt64 x)
{
    return intHashCRC32(x);
}

ALWAYS_INLINE inline UInt64 slotHash(UInt32 x)
{
    return intHashCRC32(x);
}

ALWAYS_INLINE inline UInt64 slotHash(UInt128 x)
{
    return static_cast<UInt64>(x);
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

        size_t place = slotHash(key) & mask;
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
    size_t count = 0;
    bool has_zero = false;
};

/// A stream switches from its local set to the routing once the set would outgrow this size.
constexpr size_t MAX_LOCAL_SET_BYTES = 1024 * 1024;

/// A stream deduplicates its buffered keys once it has buffered this many keys for the first time.
constexpr size_t INITIAL_COMPACTION_BUDGET = 64 * 1024;

/// The number of buffered keys of a stream that are deduplicated first to estimate whether the deduplication of
/// all of them is worth it.
constexpr size_t COMPACTION_SAMPLE_KEYS = 4096;

/// After a deduplication, the budget is this many times the number of keys that remain buffered.
constexpr size_t COMPACTION_BUDGET_FACTOR = 4;

/// The number of partitions per stream. Many partitions keep the partition sets cache-resident even when there are
/// many distinct keys, and they balance the build between the threads.
constexpr size_t PARTITIONS_PER_STREAM = 16;

/// The blocks of a chain of buffered keys grow from `MIN_BLOCK_SIZE` to `MIN_BLOCK_SIZE << MAX_BLOCK_CLASS` keys,
/// so that the chains of the partitions with few keys stay small.
constexpr size_t MIN_BLOCK_SIZE = 16;
constexpr size_t MAX_BLOCK_CLASS = 6;

template <typename Key>
class RadixUniqExactState final : public IRadixUniqExactState
{
public:
    RadixUniqExactState(size_t num_streams_, size_t partition_bits_)
        : partition_bits(partition_bits_)
        , num_partitions(1ULL << partition_bits_)
        , streams(num_streams_)
    {
        for (auto & stream : streams)
            stream.local_set.reset(INITIAL_LOCAL_SET_CAPACITY);
    }

    /// Calls `insert` for every key (after skipping NULLs and consecutive duplicates) of the column.
    /// `extract` maps the row number to the key, `same` compares two rows.
    template <typename Extract, typename Same>
    ALWAYS_INLINE void routeRows(size_t stream_index, size_t num_rows, const UInt8 * null_map, Extract && extract, Same && same)
    {
        auto & stream = streams[stream_index];
        size_t last_row = 0;
        bool has_last_row = false;

        for (size_t row = 0; row < num_rows; ++row)
        {
            if (null_map && null_map[row])
                continue;

            if (has_last_row && same(last_row, row))
                continue;
            last_row = row;
            has_last_row = true;

            insert(stream, extract(row));
        }

        rows.fetch_add(num_rows, std::memory_order_relaxed);
    }

    /// The same as `routeRows` for the values that are the keys themselves (zero-extended). Consecutive duplicates
    /// and NULLs are dropped without branches into a buffer, which is then inserted.
    template <typename T>
    void routeValues(size_t stream_index, const T * data, size_t num_rows, const UInt8 * null_map)
    {
        auto & stream = streams[stream_index];
        std::array<Key, 256> keys; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - only the first num_keys entries are read
        T last_value{};
        bool has_last_value = false;

        for (size_t begin = 0; begin < num_rows; begin += keys.size())
        {
            const size_t end = std::min(begin + keys.size(), num_rows);
            size_t num_keys = 0;
            for (size_t row = begin; row < end; ++row)
            {
                const T value = data[row];
                const bool is_null = null_map && null_map[row];
                keys[num_keys] = static_cast<Key>(value);
                num_keys += !is_null && !(has_last_value && value == last_value);
                last_value = is_null ? last_value : value;
                has_last_value |= !is_null;
            }

            for (size_t i = 0; i < num_keys; ++i)
                insert(stream, keys[i]);
        }

        rows.fetch_add(num_rows, std::memory_order_relaxed);
    }

    void route(size_t stream_index, const IColumn & column, const UInt8 * null_map) override;

    void finishStream(size_t stream_index) override
    {
        auto & stream = streams[stream_index];
        if (stream.mode == Mode::Radix)
            return;

        /// The local set holds distinct keys, so no chain needs a deduplication while it is moved.
        switchToRadixMode(stream);
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

        if (total == 0)
            return 0;

        /// The load factor is at most 0.5, the actual number of distinct keys can only be lower.
        FlatSet<Key> set;
        set.reset(std::bit_ceil(total * 2));

        for (const auto & stream : streams)
        {
            for (const Block * block = stream.chains[partition].head; block; block = block->next)
            {
                const Key * keys = block->keys();
                for (size_t i = 0; i < block->size; ++i)
                    set.insert(keys[i]);
            }
        }

        return set.size();
    }

    size_t numPartitions() const override { return num_partitions; }

private:
    static constexpr size_t INITIAL_LOCAL_SET_CAPACITY = 256;

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
    };

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
        stream.num_buffered = stream.local_set.size();
        stream.compaction_budget = std::max(INITIAL_COMPACTION_BUDGET, stream.num_buffered * COMPACTION_BUDGET_FACTOR);
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
        size_t sampled_before = 0;
        size_t sampled_after = 0;
        size_t num_compacted = 0;
        for (; num_compacted < num_partitions && sampled_before < COMPACTION_SAMPLE_KEYS; ++num_compacted)
        {
            auto & chain = stream.chains[stream.next_sample_chain];
            stream.next_sample_chain = (stream.next_sample_chain + 1) % num_partitions;
            sampled_before += chain.size;
            compact(stream, chain);
            sampled_after += chain.size;
        }
        stream.num_buffered -= sampled_before - sampled_after;

        if (sampled_after * 4 <= sampled_before)
        {
            switchToUnboundedSetMode(stream, sampled_before, sampled_after);
            return;
        }

        if (sampled_after * 4 > sampled_before * 3)
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

    /// Most of the keys of the stream are repeated, so they are better deduplicated right away, as the ordinary
    /// aggregation does, even if the set does not fit into the cache: moves the buffered keys into the local set,
    /// which grows without a limit since then. The keys are moved to the chains when the stream finishes.
    void switchToUnboundedSetMode(Stream & stream, size_t sampled_before, size_t sampled_after)
    {
        const size_t estimated_size = stream.num_buffered * sampled_after / sampled_before + 1;
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
        {
            const Key * keys = block->keys();
            for (size_t i = 0; i < block->size; ++i)
                set.insert(keys[i]);
        }

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
    }

    const size_t partition_bits;
    const size_t num_partitions;
    std::vector<Stream> streams;
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

/// Strings and `IPv6` are represented by their 128-bit `SipHash`, as `uniqExact` does, and other 16-byte values by a bijection of themselves.
/// Consecutive duplicates are compared by their bytes before they are hashed.
template <>
void RadixUniqExactState<UInt128>::route(size_t stream, const IColumn & column, const UInt8 * null_map)
{
    auto sip_hash = [](const char * data, size_t size)
    {
        SipHash hash;
        hash.update(data, size);
        return hash.get128();
    };

    if (const auto * column_string = typeid_cast<const ColumnString *>(&column))
    {
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row)
            {
                const auto value = column_string->getDataAt(row);
                return sip_hash(value.data(), value.size());
            },
            [&](size_t lhs, size_t rhs) { return column_string->getDataAt(lhs) == column_string->getDataAt(rhs); });
    }
    else if (const auto * column_fixed_string = typeid_cast<const ColumnFixedString *>(&column))
    {
        const char * chars = reinterpret_cast<const char *>(column_fixed_string->getChars().data());
        const size_t n = column_fixed_string->getN();
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row) { return sip_hash(chars + row * n, n); },
            [&](size_t lhs, size_t rhs) { return memcmp(chars + lhs * n, chars + rhs * n, n) == 0; });
    }
    else if (const auto * column_ipv6 = typeid_cast<const ColumnIPv6 *>(&column))
    {
        const auto & data = column_ipv6->getData();
        routeRows(
            stream, column.size(), null_map,
            [&](size_t row) { return sip_hash(reinterpret_cast<const char *>(&data[row]), sizeof(IPv6)); },
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

size_t partitionBits(size_t num_streams)
{
    /// At most 4096 partitions, because every stream keeps a chain per partition.
    return std::clamp<size_t>(std::bit_width(num_streams * PARTITIONS_PER_STREAM - 1), 8, 12);
}

}

RadixUniqExactStatePtr createRadixUniqExactState(const IDataType & argument_type, size_t num_streams)
{
    const IDataType & type = *removeNullable(argument_type.shared_from_this());
    WhichDataType which(type);

    /// The types for which `uniqExact` keeps the value itself in the set.
    if (which.isNativeInteger() || which.isFloat() || which.isEnum() || which.isDate() || which.isDate32() || which.isDateTime() || which.isIPv4())
    {
        const size_t size = type.getSizeOfValueInMemory();
        if (size <= sizeof(UInt32))
            return std::make_shared<RadixUniqExactState<UInt32>>(num_streams, partitionBits(num_streams));
        if (size == sizeof(UInt64))
            return std::make_shared<RadixUniqExactState<UInt64>>(num_streams, partitionBits(num_streams));
        return nullptr;
    }

    if (which.isUInt128() || which.isInt128() || which.isUUID() || which.isString() || which.isFixedString() || which.isIPv6())
        return std::make_shared<RadixUniqExactState<UInt128>>(num_streams, partitionBits(num_streams));

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

    const auto column = current_chunk.getColumns()[key_position]->convertToFullIfWrapped();
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

    /// Like the aggregation without keys, return a single row even for an empty input, unless asked otherwise.
    if (empty_result_for_empty_set && state->numRows() == 0)
        return {};

    auto column = ColumnUInt64::create();
    column->insertValue(total);
    return Chunk(Columns{std::move(column)}, 1);
}

}
