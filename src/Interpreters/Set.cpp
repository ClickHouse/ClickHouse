#include <array>
#include <atomic>
#include <bit>
#include <limits>
#include <optional>
#include <shared_mutex>

#include <Core/Field.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>

#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>
#include <base/unaligned.h>
#include <fmt/format.h>
#include <Columns/ColumnDecimal.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Interpreters/Set.h>
#include <Interpreters/DiskSet.h>
#include <Interpreters/DiskSetBuilder.h>
#include <Interpreters/DiskSetKeys.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataTypes/NullableUtils.h>
#include <Interpreters/sortBlock.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/Context.h>

#include <Processors/Chunk.h>

#include <Storages/MergeTree/KeyCondition.h>

#include <base/arithmeticOverflow.h>
#include <base/range.h>
#include <base/sort.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace ProfileEvents
{
    extern const Event SetsSpilledToDisk;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

/// The mask of the first `bytes` bytes of a 64-bit word loaded from memory.
constexpr UInt64 firstBytesMask(size_t bytes)
{
    if (bytes == 0)
        return 0;
    if (bytes >= 8)
        return ~0ULL;
    if constexpr (std::endian::native == std::endian::little)
        return (1ULL << (8 * bytes)) - 1;
    else
        return ~0ULL << (64 - 8 * bytes);
}

constexpr auto low_masks = []
{
    std::array<UInt64, 17> masks{};
    for (size_t size = 0; size <= 16; ++size)
        masks[size] = firstBytesMask(std::min<size_t>(size, 8));
    return masks;
}();

constexpr auto high_masks = []
{
    std::array<UInt64, 17> masks{};
    for (size_t size = 0; size <= 16; ++size)
        masks[size] = firstBytesMask(size > 8 ? size - 8 : 0);
    return masks;
}();

}

/// A cuckoo hash table of the keys of a set, looked up without data-dependent branches instead of the hash table
/// (see `execute`). Each key is in one of its two slots, and every slot without a key holds a copy of some key of
/// the set, so a value is in the set if and only if it equals one of its two slots.
class Set::CuckooLookup
{
public:
    /// The table is built by a single lookup on the query's critical path, so only small sets get one.
    static constexpr size_t max_keys = 1 << 15;
    static constexpr size_t max_bytes = 8 << 20;
    static constexpr size_t max_string_size = 15;

    static constexpr size_t minSlots(size_t num_keys) { return 2 * num_keys + num_keys / 4; }

    size_t allocatedBytes() const
    {
        return slots32.allocated_bytes() + slots64.allocated_bytes() + slots128.allocated_bytes() + slots256.allocated_bytes();
    }

    /// Builds nothing if the peak memory of a placement would exceed `memory_budget`.
    template <typename Method>
    static std::unique_ptr<CuckooLookup> tryCreate(const Method & method, size_t num_keys, size_t memory_budget, size_t & attempts)
    {
        using Key = typename Method::Key;
        attempts = 0;
        auto lookup = std::make_unique<CuckooLookup>();
        if constexpr (IsSetMethodOneNumber<Method>::value || IsSetMethodKeysFixed<Method>::value)
        {
            if constexpr (std::is_same_v<Key, UInt32> || std::is_same_v<Key, UInt64> || std::is_same_v<Key, UInt128>
                          || std::is_same_v<Key, UInt256>)
            {
                if (attemptPeakBytes(num_keys, sizeof(Key), minBits(num_keys), 0) > memory_budget)
                    return nullptr;
                std::vector<Key> keys;
                keys.reserve(num_keys);
                for (auto it = method.data.begin(); it != method.data.end(); ++it)
                    keys.push_back(it->getKey());
                if (lookup->tryBuildTable(keys, memory_budget, attempts))
                    return lookup;
            }
        }
        else if constexpr (IsSetMethodString<Method>::value)
        {
            if (attemptPeakBytes(num_keys, sizeof(UInt128), minBits(num_keys), 0) > memory_budget)
                return nullptr;
            std::vector<UInt128> keys;
            keys.reserve(num_keys);
            for (auto it = method.data.begin(); it != method.data.end(); ++it)
            {
                const std::string_view key = it->getKey();
                if (key.size() > max_string_size)
                    return nullptr;
                /// `encode` reads 16 bytes.
                char buf[16] = {};
                memcpy(buf, key.data(), key.size());
                keys.push_back(encode(reinterpret_cast<const UInt8 *>(buf), key.size()));
            }
            if (lookup->tryBuildTable(keys, memory_budget, attempts))
                return lookup;
        }
        return nullptr;
    }

    /// The same result as `executeImplCase`. Returns false if the table is not for this method and columns.
    template <typename Method>
    bool execute(
        const Method & method, const ColumnRawPtrs & columns, const Sizes & sizes,
        ColumnUInt8::Container & vec_res, bool negative, ConstNullMapPtr null_map) const
    {
        return mixed_hash ? executeWithHash<true>(method, columns, sizes, vec_res, negative, null_map)
                          : executeWithHash<false>(method, columns, sizes, vec_res, negative, null_map);
    }

private:
    template <bool mixed, typename Method>
    bool executeWithHash(
        const Method &, const ColumnRawPtrs & columns, const Sizes & sizes,
        ColumnUInt8::Container & vec_res, bool negative, ConstNullMapPtr null_map) const
    {
        using Key = typename Method::Key;
        const size_t rows = vec_res.size();
        UInt8 * res = vec_res.data();

        if constexpr (IsSetMethodOneNumber<Method>::value && (std::is_same_v<Key, UInt32> || std::is_same_v<Key, UInt64>))
        {
            const IColumn & column = *columns[0];
            if (getSlots<Key>().empty() || !column.isFixedAndContiguous() || column.sizeOfValueIfFixed() != sizeof(Key))
                return false;
            /// Read through the method's state, so the values are those the hash table would look up.
            typename Method::State state(columns, sizes, nullptr);
            static_assert(sizeof(*state.getKeyData()) == sizeof(Key));
            findNumbers<mixed, Key>(reinterpret_cast<const char *>(state.getKeyData()), rows, res);
        }
        else if constexpr (IsSetMethodKeysFixed<Method>::value
                           && (std::is_same_v<Key, UInt32> || std::is_same_v<Key, UInt64> || std::is_same_v<Key, UInt128>
                               || std::is_same_v<Key, UInt256>))
        {
            if (getSlots<Key>().empty())
                return false;
            typename Method::State state(columns, sizes, nullptr);
            Arena pool;
            for (size_t i = 0; i < rows; ++i)
                res[i] = contains<mixed, Key>(state.getKeyHolder(i, pool));
        }
        else if constexpr (IsSetMethodString<Method>::value)
        {
            const auto * column_string = typeid_cast<const ColumnString *>(columns[0]);
            if (slots128.empty() || !column_string)
                return false;
            const UInt8 * chars = column_string->getChars().data();
            const IColumn::Offsets & offsets = column_string->getOffsets();
            /// Reading 16 bytes past the start of the last string stays within the padding of the column.
            IColumn::Offset begin = 0;
            for (size_t i = 0; i < rows; ++i)
            {
                const IColumn::Offset end = offsets[i];
                res[i] = contains<mixed, UInt128>(encode(chars + begin, end - begin));
                begin = end;
            }
        }
        else
            return false;

        /// A row with a NULL key is not in the set, whatever its value.
        if (null_map)
        {
            const UInt8 * is_null = null_map->data();
            for (size_t i = 0; i < rows; ++i)
                res[i] = static_cast<UInt8>(res[i] & !is_null[i]);
        }

        if (negative)
        {
            for (size_t i = 0; i < rows; ++i)
                res[i] ^= 1;
        }

        return true;
    }

    template <typename T> struct IsSetMethodOneNumber : std::false_type {};
    template <typename FieldType, typename Data, bool use_cache>
    struct IsSetMethodOneNumber<SetMethodOneNumber<FieldType, Data, use_cache>> : std::true_type {};

    template <typename T> struct IsSetMethodKeysFixed : std::false_type {};
    template <typename Data, bool has_nullable_keys>
    struct IsSetMethodKeysFixed<SetMethodKeysFixed<Data, has_nullable_keys>> : std::true_type {};

    template <typename T> struct IsSetMethodString : std::false_type {};
    template <typename Data>
    struct IsSetMethodString<SetMethodString<Data>> : std::true_type {};

    PODArray<UInt32> slots32;
    PODArray<UInt64> slots64;
    PODArray<UInt128> slots128;
    PODArray<UInt256> slots256;
    /// One per 64-bit word of a key. With `mixed_hash`, they apply to `intHashCRC32` of a number key, and a longer
    /// key takes its two slots from the high and low bits of the sum of `intHash64(word ^ multipliers1[i])`.
    UInt64 multipliers1[4] = {};
    UInt64 multipliers2[4] = {};
    bool mixed_hash = false;
    size_t shift = 0;
    UInt64 mask = 0;

    static UInt64 splitMix64(UInt64 & state)
    {
        UInt64 z = (state += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

    /// The bytes of the string and, in the last byte, its length. A string longer than 15 bytes gets the length 16,
    /// which no key has.
    static UInt128 encode(const UInt8 * data, size_t size)
    {
        const size_t clamped = std::min<size_t>(size, 16);
        const UInt64 low = unalignedLoad<UInt64>(data) & low_masks[clamped];
        UInt64 high = unalignedLoad<UInt64>(data + 8) & high_masks[clamped];
        if constexpr (std::endian::native == std::endian::little)
            high = (high & ~(0xFFULL << 56)) | (static_cast<UInt64>(clamped) << 56);
        else
            high = (high & ~0xFFULL) | clamped;
        UInt128 key;
        key.items[0] = low;
        key.items[1] = high;
        return key;
    }

    template <typename Key>
    PODArray<Key> & getSlots()
    {
        if constexpr (std::is_same_v<Key, UInt32>)
            return slots32;
        else if constexpr (std::is_same_v<Key, UInt64>)
            return slots64;
        else if constexpr (std::is_same_v<Key, UInt128>)
            return slots128;
        else
            return slots256;
    }

    template <typename Key>
    const PODArray<Key> & getSlots() const
    {
        return const_cast<CuckooLookup *>(this)->getSlots<Key>();
    }

    template <bool mixed, typename Key>
    std::pair<size_t, size_t> places(const Key & key) const
    {
        static constexpr size_t num_words = sizeof(Key) <= sizeof(UInt64) ? 1 : sizeof(Key) / sizeof(UInt64);
        auto word = [&](size_t i) -> UInt64
        {
            if constexpr (sizeof(Key) <= sizeof(UInt64))
                return static_cast<UInt64>(key);
            else
                return key.items[i];
        };

        if constexpr (mixed && sizeof(Key) <= sizeof(UInt64))
        {
            const UInt64 hash = intHashCRC32(static_cast<UInt64>(key));
            return {(hash * multipliers1[0]) >> shift, (hash * multipliers2[0]) >> shift};
        }
        else if constexpr (mixed)
        {
            UInt64 hash = 0;
            for (size_t i = 0; i < num_words; ++i)
                hash += intHash64(word(i) ^ multipliers1[i]);
            return {hash >> shift, hash & mask};
        }
        else
        {
            UInt64 hash1 = 0;
            UInt64 hash2 = 0;
            for (size_t i = 0; i < num_words; ++i)
            {
                hash1 += word(i) * multipliers1[i];
                hash2 += word(i) * multipliers2[i];
            }
            return {hash1 >> shift, hash2 >> shift};
        }
    }

    template <bool mixed, typename Key>
    UInt8 contains(const Key & key) const
    {
        const Key * slots = getSlots<Key>().data();
        const auto [place1, place2] = places<mixed>(key);
        return static_cast<UInt8>(static_cast<UInt8>(slots[place1] == key) | static_cast<UInt8>(slots[place2] == key));
    }

    static size_t minBits(size_t num_keys)
    {
        size_t bits = 4;
        while ((1ULL << bits) < minSlots(num_keys))
            ++bits;
        return bits;
    }

    /// The peak memory of a placement into `1 << bits` slots: the keys, the slots and their occupancy bytes, or, while
    /// the slots of the previous placement (none if `previous_bits` is 0) are reallocated, both blocks of slots.
    static size_t attemptPeakBytes(size_t num_keys, size_t key_size, size_t bits, size_t previous_bits)
    {
        const size_t slot_bytes = (1ULL << bits) * key_size;
        const size_t previous_slot_bytes = previous_bits ? (1ULL << previous_bits) * key_size : 0;
        const size_t occupancy_bytes = 1ULL << bits;
        return num_keys * key_size + std::max(previous_slot_bytes + slot_bytes, slot_bytes + occupancy_bytes);
    }

    template <typename Key>
    bool tryBuildTable(const std::vector<Key> & keys, size_t memory_budget, size_t & attempts)
    {
        if (keys.empty())
            return false;

        size_t bits = minBits(keys.size());
        size_t previous_bits = 0;
        UInt64 random_state = keys.size();
        for (size_t attempt = 0; attempt < 16; ++attempt)
        {
            if (attempt > 0 && attempt % 4 == 0)
                ++bits;
            if ((1ULL << bits) * sizeof(Key) > max_bytes || attemptPeakBytes(keys.size(), sizeof(Key), bits, previous_bits) > memory_budget)
                break;
            for (size_t i = 0; i < 4; ++i)
            {
                multipliers1[i] = splitMix64(random_state) | 1;
                multipliers2[i] = splitMix64(random_state) | 1;
            }
            ++attempts;
            mixed_hash = attempt > 0;
            if (mixed_hash ? tryPlaceKeys<true>(keys, bits) : tryPlaceKeys<false>(keys, bits))
                return true;
            previous_bits = bits;
        }
        getSlots<Key>().clear();
        return false;
    }

    template <bool mixed, typename Key>
    bool tryPlaceKeys(const std::vector<Key> & keys, size_t bits)
    {
        shift = 64 - bits;
        mask = (1ULL << bits) - 1;
        const size_t num_slots = 1ULL << bits;
        auto & slots = getSlots<Key>();
        /// The slots that get no key keep this copy of a key of the set.
        slots.assign(num_slots, keys[0]);
        PODArray<UInt8> occupied(num_slots, 0);

        /// The moves would never end if more than two keys had the same two slots.
        static constexpr size_t max_moves = 100;
        static constexpr size_t prefetch_distance = 16;
        const size_t num_keys = keys.size();
        for (size_t i = 0; i < num_keys; ++i)
        {
            if (i + prefetch_distance < num_keys)
            {
                const size_t ahead = places<mixed>(keys[i + prefetch_distance]).first;
                __builtin_prefetch(&occupied[ahead], 1);
                __builtin_prefetch(&slots[ahead], 1);
            }

            Key key = keys[i];
            size_t current = places<mixed>(key).first;
            for (size_t moves = 0;; ++moves)
            {
                if (!occupied[current])
                {
                    occupied[current] = 1;
                    slots[current] = key;
                    break;
                }
                if (moves == max_moves)
                    return false;
                std::swap(key, slots[current]);
                const auto [first, second] = places<mixed>(key);
                current = current == first ? second : first;
            }
        }

        return true;
    }

    template <bool mixed, typename Key>
    void findNumbers(const char * values, size_t rows, UInt8 * res) const
    {
        const Key * slots = getSlots<Key>().data();
        const UInt64 m1 = multipliers1[0];
        [[maybe_unused]] const UInt64 m2 = multipliers2[0];
        const size_t s = shift;
        [[maybe_unused]] const UInt64 m = mask;
        /// Must match `places`.
        auto places_of = [&](Key v) -> std::pair<size_t, size_t>
        {
            if constexpr (mixed)
            {
                const UInt64 hash = intHashCRC32(static_cast<UInt64>(v));
                return {(hash * m1) >> s, (hash * m2) >> s};
            }
            else
                return {(static_cast<UInt64>(v) * m1) >> s, (static_cast<UInt64>(v) * m2) >> s};
        };

        size_t i = 0;
        for (; i + 4 <= rows; i += 4)
        {
            const Key v0 = unalignedLoad<Key>(values + (i + 0) * sizeof(Key));
            const Key v1 = unalignedLoad<Key>(values + (i + 1) * sizeof(Key));
            const Key v2 = unalignedLoad<Key>(values + (i + 2) * sizeof(Key));
            const Key v3 = unalignedLoad<Key>(values + (i + 3) * sizeof(Key));
            const auto [p0, q0] = places_of(v0);
            const auto [p1, q1] = places_of(v1);
            const auto [p2, q2] = places_of(v2);
            const auto [p3, q3] = places_of(v3);
            const Key a0 = slots[p0];
            const Key a1 = slots[p1];
            const Key a2 = slots[p2];
            const Key a3 = slots[p3];
            const Key b0 = slots[q0];
            const Key b1 = slots[q1];
            const Key b2 = slots[q2];
            const Key b3 = slots[q3];
            res[i + 0] = static_cast<UInt8>((a0 == v0) | (b0 == v0));
            res[i + 1] = static_cast<UInt8>((a1 == v1) | (b1 == v1));
            res[i + 2] = static_cast<UInt8>((a2 == v2) | (b2 == v2));
            res[i + 3] = static_cast<UInt8>((a3 == v3) | (b3 == v3));
        }

        for (; i < rows; ++i)
            res[i] = contains<mixed, Key>(unalignedLoad<Key>(values + i * sizeof(Key)));
    }
};

Set::Set(const SizeLimits & limits_, size_t max_elements_to_fill_, bool transform_null_in_)
    :  limits(limits_), transform_null_in(transform_null_in_), max_elements_to_fill(max_elements_to_fill_)
    , log(getLogger("Set")), cast_cache(std::make_unique<InternalCastFunctionCache>())
{}


Set::~Set() = default;

void Set::setSpillSettings(SetSpillSettings spill_settings_)
{
    std::lock_guard lock(rwlock);
    chassert(!isCreated());
    spill_settings = std::move(spill_settings_);
}

void Set::finishInsert()
{
    /// Consumers may already expose this set to the scheduler while its builder is still running.
    std::lock_guard lock(rwlock);
    if (disk_set_builder)
    {
        disk_set = disk_set_builder->finish();
        is_truncated = disk_set_builder->isTruncated();
        disk_set_builder.reset();
    }
    else if (data && !cuckoo_lookup)
    {
        const size_t num_keys = data->getTotalRowCount();
        using Type = SetVariants::Type;
        const bool has_suitable_key = data->type == Type::key32 || data->type == Type::key64 || data->type == Type::key_string
            || data->type == Type::keys32 || data->type == Type::keys64 || data->type == Type::keys128 || data->type == Type::keys256
            || data->type == Type::nullable_keys128 || data->type == Type::nullable_keys256;
        rows_before_cuckoo_lookup = has_suitable_key && num_keys > 0 && num_keys <= CuckooLookup::max_keys && !cuckoo_lookup_disabled
            ? static_cast<Int64>(rows_per_key_before_cuckoo_lookup * num_keys)
            : -1;
    }
    is_created = true;
}

std::unique_ptr<Set::CuckooLookup> Set::createCuckooLookup() const
{
    const size_t num_keys = data->getTotalRowCount();

    /// The build takes at most 1/8 of the memory the query can still use and of the memory left before the set spills.
    size_t memory_budget = std::numeric_limits<size_t>::max();
    if (const auto available = getCurrentQueryAvailableMemory())
        memory_budget = *available / 8;
    if (const size_t threshold = spill_settings.max_bytes_before_external_set)
    {
        const size_t used = static_cast<size_t>(std::max<Int64>(0, getCurrentQueryMemoryUsage()));
        memory_budget = std::min(memory_budget, (threshold - std::min(threshold, used)) / 8);
    }

    Stopwatch watch;
    size_t attempts = 0;
    auto lookup = data->callOnMethod([&](const auto & method) { return CuckooLookup::tryCreate(method, num_keys, memory_budget, attempts); });
    LOG_TRACE(log, "{} the lookup table for {} keys: {} placements tried, {} bytes, {:.3f} ms",
        lookup ? "Built" : "Did not build", num_keys, attempts, lookup ? lookup->allocatedBytes() : 0, watch.elapsedSeconds() * 1000);
    return lookup;
}

ProcessorMemoryStats Set::getMemoryStatsUnlocked() const
{
    if (!spill_settings.tmp_data || !data || data->empty() || !data->getTotalRowCount()
        || spill_after_build_started.load())
        return {};

    ProcessorMemoryStats stats;
    stats.spillable_memory_bytes = data->getTotalByteCount() + (cuckoo_lookup ? cuckoo_lookup->allocatedBytes() : 0);
    stats.need_reserved_memory_bytes = estimateMemoryToSpill(/*requested=*/ true);

    /// A built set writes its disk set while lookups keep reading the table, so the directory and the Bloom
    /// filters of the disk set take memory besides the table until the disk set replaces it. A set that
    /// spills while it is built frees the table first and creates them only when it finishes.
    if (isCreated())
        stats.need_reserved_memory_bytes += estimateMemoryOfDiskSet();
    return stats;
}

ProcessorMemoryStats Set::getMemoryStats() const
{
    std::shared_lock lock(rwlock);
    return getMemoryStatsUnlocked();
}

size_t Set::spill(size_t /*at_least_bytes*/)
{
    std::unique_lock lock(rwlock);
    const size_t bytes = getMemoryStatsUnlocked().spillable_memory_bytes;
    if (!bytes)
        return 0;

    if (isCreated())
    {
        lock.unlock();
        return spillAfterBuild("memory scheduler request", /*requested=*/ true);
    }

    spillWhileBuilding("memory scheduler request", /*requested=*/ true);
    return bytes;
}

bool Set::isLargeEnoughToSpill() const
{
    return data->getTotalByteCount() >= DiskSetBuilder::getMinBytesInRun(spill_settings.max_bytes_before_external_set);
}

bool Set::isSpillNeededBeforeInsert(const ColumnRawPtrs & key_columns, size_t rows) const
{
    const size_t threshold = spill_settings.max_bytes_before_external_set;

    if (!threshold || !isLargeEnoughToSpill())
        return false;

    /// Query accounting can briefly become negative while a concurrent free saturates its counter.
    const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
    const UInt64 available_memory = threshold - std::min<UInt64>(threshold, query_memory_usage);

    /// Spilling needs memory while the table still exists.
    const size_t spill_memory = estimateMemoryToSpill();
    if (spill_memory > available_memory)
        return true;

    /// The insertion grows the table and its arena, and the state of a method with fixed keys packs the keys of
    /// the whole chunk before inserting them.
    size_t insert_memory = data->estimateGrowthMemory(key_columns, /*start_row=*/0, rows);
    if (common::addOverflow(insert_memory, data->estimatePreparedKeysMemory(rows, key_sizes), insert_memory))
        return true;

    if (fill_set_elements)
    {
        /// Explicit elements copy the new keys.
        for (const auto * column : key_columns)
        {
            if (common::addOverflow(insert_memory, column->byteSize(), insert_memory))
                return true;
        }
    }

    return insert_memory > available_memory - spill_memory;
}

void Set::spillWhileBuilding(std::string_view reason, bool requested)
{
    logSpill("while it is built", reason);

    /// `fill_set_elements` mainly serves index analysis. Since the set itself has to be spilled, it is safe
    /// to assume that its elements would not fit in memory for index analysis later either. Index analysis
    /// could possibly work on disk, but this is not implemented yet.
    fill_set_elements = false;
    set_elements.clear();

    data->callOnMethod([&]<typename Method>(const Method & method)
    {
        compute_disk_set_keys = computeDiskSetKeys<Method>;
        disk_set_builder = createDiskSetBuilderWithKeys(method, limits, requested);
    });

    data.reset();
    ProfileEvents::increment(ProfileEvents::SetsSpilledToDisk);
}

bool Set::isSpillNeeded() const
{
    return spill_settings.max_bytes_before_external_set
        && getCurrentQueryMemoryUsage() > static_cast<Int64>(spill_settings.max_bytes_before_external_set)
        && isLargeEnoughToSpill();
}

bool Set::isSpillNeededAfterBuild() const
{
    return spill_settings.max_bytes_before_external_set && isLargeEnoughToSpill()
        && getCurrentQueryMemoryUsage() + static_cast<Int64>(estimateMemoryOfDiskSet())
        > static_cast<Int64>(spill_settings.max_bytes_before_external_set);
}

size_t Set::estimateMemoryOfDiskSet() const
{
    return data->callOnMethod(
        [&]<typename Method>(const Method &)
        { return estimateDiskSetMemory<DiskSetKey<Method>>(data->getTotalRowCount(), spill_settings.max_bytes_for_bloom_filter); });
}

size_t Set::spillAfterBuild(std::string_view reason, bool requested) const
{
    if (spill_after_build_started.exchange(true))
        return 0;

    size_t bytes = 0;
    std::unique_ptr<DiskSet> new_disk_set;
    ComputeDiskSetKeys new_compute_disk_set_keys = nullptr;
    {
        /// The table does not change after the build, so other lookups keep reading it while its keys are
        /// written.
        std::shared_lock lock(rwlock);

        /// Only the lookup that started the spill replaces the table, so the table is still in memory here.
        chassert(data && !disk_set);
        bytes = data->getTotalByteCount();
        logSpill("while it is used", reason);

        data->callOnMethod([&]<typename Method>(const Method & method)
        {
            /// The keys are final, so the builder applies no size limits and keeps all of them.
            new_disk_set = createDiskSetBuilderWithKeys(method, SizeLimits{}, requested)->finish();
            new_compute_disk_set_keys = computeDiskSetKeys<Method>;
        });
    }

    /// It is freed when the function returns, after the lock is released, so the lookups waiting for the lock
    /// do not also wait for the table to be freed.
    std::unique_ptr<SetVariants> table;
    {
        /// The exclusive lock waits for the lookups in progress, which hold the shared lock for one chunk
        /// each.
        std::lock_guard lock(rwlock);
        compute_disk_set_keys = new_compute_disk_set_keys;
        disk_set = std::move(new_disk_set);
        table = std::move(data);
        if (cuckoo_lookup)
            bytes += cuckoo_lookup->allocatedBytes();
        cuckoo_lookup.reset();
        rows_before_cuckoo_lookup = -1;
        ++keys_version;
    }
    ProfileEvents::increment(ProfileEvents::SetsSpilledToDisk);
    return bytes;
}

void Set::logSpill(std::string_view stage, std::string_view reason) const
{
    LOG_TRACE(log, "Switching the set of IN to external mode {}: {} "
        "(keys in memory: {}, set memory: {}, query memory: {}, spill threshold: {})",
        stage,
        reason,
        data->getTotalRowCount(),
        formatReadableSizeWithBinarySuffix(data->getTotalByteCount()),
        formatReadableSizeWithBinarySuffix(std::max<Int64>(0, getCurrentQueryMemoryUsage())),
        formatReadableSizeWithBinarySuffix(spill_settings.max_bytes_before_external_set));
}

size_t Set::getBuilderSpillThreshold(bool requested) const
{
    return requested ? 0 : spill_settings.max_bytes_before_external_set;
}

size_t Set::estimateMemoryToSpill(bool requested) const
{
    const size_t buffer_size = spill_settings.tmp_data->getSettings().buffer_size;
    return data->callOnMethod(
        [&]<typename Method>(const Method &)
        {
            return estimateMemoryToComputeDiskSetKeysOfTable<Method>(spill_settings.max_block_size)
                + DiskSetBuilder::estimateMemoryToWriteRun(
                    getBuilderSpillThreshold(requested), spill_settings.max_block_size, sizeof(DiskSetKey<Method>),
                    buffer_size);
        });
}

template <typename Method>
std::unique_ptr<DiskSetBuilder> Set::createDiskSetBuilderWithKeys(
    const Method & method, const SizeLimits & builder_limits, bool requested) const
{
    /// The runs of the external sort of a set use `temporary_files_codec`, as other external sorts do, while
    /// the file of the set is never compressed (see `DiskSetImpl::createFileScope`). For sets of numbers,
    /// `LZ4` makes the runs 2 to 4 times smaller, and the builds 8-22% slower while the runs stay in the page
    /// cache. The disk keys of the methods without fixed-size keys are mostly hash bits, which compress too
    /// little to pay for compressing the runs. `LZ4` makes 128-bit hashes slightly larger, and `ZSTD` saves
    /// only about 2%. The disk keys of strings share their first bytes with their neighbors, but `LZ4` saves
    /// only about 6% of their runs and makes the queries that build such sets about 12% slower. So the runs
    /// of such a set are written uncompressed, whatever `temporary_files_codec` is.
    TemporaryDataOnDiskScopePtr tmp_data = spill_settings.tmp_data;
    if constexpr (!set_method_has_fixed_size_keys<Method>)
        tmp_data = tmp_data->childScope(tmp_data->getSettings().metrics, /*buffer_size_=*/0, "NONE");

    auto builder = createDiskSetBuilder<DiskSetKey<Method>>(
        std::move(tmp_data),
        builder_limits,
        getBuilderSpillThreshold(requested),
        spill_settings.max_block_size,
        spill_settings.min_free_disk_space,
        spill_settings.max_bytes_for_bloom_filter,
        spill_settings.max_bytes_for_cache,
        spill_settings.process_list_element);

    computeDiskSetKeysOfTable(method, data_types, key_sizes, spill_settings.max_block_size, [&](ColumnPtr keys)
    {
        builder->add(std::move(keys));
    });

    return builder;
}

void Set::executeDiskSet(const ColumnRawPtrs & key_columns, ColumnUInt8::Container & vec_res, bool negative, ConstNullMapPtr null_map) const
{
    const size_t rows = vec_res.size();
    auto keys = compute_disk_set_keys(key_columns, key_sizes, rows, /*null_map=*/nullptr);

    disk_set->containsBatch(*keys, {vec_res.data(), rows});

    /// Keys with a `NULL` component are not in the set, as in `executeImplCase`.
    for (size_t row = 0; row < rows; ++row)
        vec_res[row] = negative ^ (vec_res[row] && !(null_map && (*null_map)[row]));
}


template <typename Method>
void NO_INLINE Set::insertFromBlockImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    ConstNullMapPtr null_map,
    ColumnUInt8::Container * out_filter)
{
    if (null_map)
    {
        if (out_filter)
            insertFromBlockImplCase<Method, true, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, true, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
    else
    {
        if (out_filter)
            insertFromBlockImplCase<Method, false, true>(method, key_columns, rows, variants, null_map, out_filter);
        else
            insertFromBlockImplCase<Method, false, false>(method, key_columns, rows, variants, null_map, out_filter);
    }
}


template <typename Method, bool has_null_map, bool build_filter>
void NO_INLINE Set::insertFromBlockImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    size_t rows,
    SetVariants & variants,
    [[maybe_unused]] ConstNullMapPtr null_map,
    [[maybe_unused]] ColumnUInt8::Container * out_filter)
{
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// For all rows
    for (size_t i = 0; i < rows; ++i)
    {
        if constexpr (has_null_map)
        {
            if ((*null_map)[i])
            {
                if constexpr (build_filter)
                {
                    (*out_filter)[i] = false;
                }
                continue;
            }
        }

        [[maybe_unused]] auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);

        if constexpr (build_filter)
            (*out_filter)[i] = emplace_result.isInserted();
    }
}


DataTypes Set::getElementTypes(DataTypes types, bool transform_null_in)
{
    for (auto & type : types)
    {
        /// Strip LowCardinality recursively to match what insertFromColumns does:
        /// it calls recursiveRemoveLowCardinality on the column side.
        type = recursiveRemoveLowCardinality(type);

        if (!transform_null_in)
            type = removeNullable(type);
    }

    return types;
}


void Set::setHeader(const ColumnsWithTypeAndName & header)
{
    std::lock_guard lock(rwlock);

    if (!data->empty())
        return;

    keys_size = header.size();
    ColumnRawPtrs key_columns;
    key_columns.reserve(keys_size);
    data_types.reserve(keys_size);
    set_elements_types.reserve(keys_size);

    /// The constant columns to the right of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        materialized_columns.emplace_back(header.at(i).column->convertToFullColumnIfConst());
        key_columns.emplace_back(materialized_columns.back().get());
        data_types.emplace_back(header.at(i).type);
        set_elements_types.emplace_back(header.at(i).type);

        /// Convert low cardinality column to full.
        if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_types.back().get()))
        {
            data_types.back() = low_cardinality_type->getDictionaryType();
            materialized_columns.emplace_back(key_columns.back()->convertToFullColumnIfLowCardinality());
            key_columns.back() = materialized_columns.back().get();
        }

        /// Strip LowCardinality recursively from set_elements_types so they match what
        /// recursiveRemoveLowCardinality does to columns in insertFromColumns.
        set_elements_types.back() = recursiveRemoveLowCardinality(set_elements_types.back());
    }

    /// We will insert to the Set only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
    {
        /// We convert nullable columns to non nullable we also need to update nullable types
        for (size_t i = 0; i < set_elements_types.size(); ++i)
        {
            data_types[i] = removeNullable(data_types[i]);
            set_elements_types[i] = removeNullable(set_elements_types[i]);
        }

        extractNestedColumnsAndNullMap(key_columns, null_map);
    }

    /// Choose data structure to use for the set.
    data->init(SetVariants::chooseMethod(key_columns, key_sizes));
}

void Set::fillSetElements()
{
    fill_set_elements = true;
    set_elements.reserve(keys_size);
    for (const auto & type : set_elements_types)
        set_elements.emplace_back(type->createColumn());
}

bool Set::insertFromBlock(const ColumnsWithTypeAndName & columns)
{
    Columns cols;
    cols.reserve(columns.size());
    for (const auto & column : columns)
        cols.emplace_back(column.column);
    return insertFromColumns(cols);
}

bool Set::insertFromColumns(const Columns & columns)
{
    /// Include explicit-element updates in the critical section: another owner may request a spill.
    std::lock_guard lock(rwlock);
    size_t rows = columns.at(0)->size();

    SetKeyColumns holder;
    /// Filter to extract distinct values from the block.
    if (fill_set_elements)
        holder.filter = ColumnUInt8::create(rows);

    bool inserted = insertFromColumnsImpl(columns, holder);
    if (inserted && fill_set_elements)
    {
        if (max_elements_to_fill && max_elements_to_fill < data->getTotalRowCount())
        {
            /// Drop filled elementes
            fill_set_elements = false;
            set_elements.clear();
        }
        else
            appendSetElements(holder);
    }

    return inserted;
}

bool Set::insertFromColumns(const Columns & columns, SetKeyColumns & holder)
{
    std::lock_guard lock(rwlock);
    return insertFromColumnsImpl(columns, holder);
}

bool Set::insertFromColumnsImpl(const Columns & columns, SetKeyColumns & holder)
{
    if (data && data->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Method Set::setHeader must be called before Set::insertFromBlock");

    cuckoo_lookup.reset();
    rows_before_cuckoo_lookup = -1;
    ++keys_version;

    holder.key_columns.reserve(keys_size);
    holder.materialized_columns.reserve(keys_size);

    /// Remember the columns we will work with
    for (size_t i = 0; i < keys_size; ++i)
    {
        holder.materialized_columns.emplace_back(recursiveRemoveLowCardinality(columns.at(i)->convertToFullIfWrapped()));
        holder.key_columns.emplace_back(holder.materialized_columns.back().get());
    }

    size_t rows = columns.at(0)->size();

    /// We will insert to the Set only keys, where all components are not NULL.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;
    if (!transform_null_in)
        null_map_holder = extractNestedColumnsAndNullMap(holder.key_columns, null_map);

    if (!disk_set_builder && isSpillNeededBeforeInsert(holder.key_columns, rows))
        spillWhileBuilding("the projected growth of the set exceeds the memory left under the spill threshold");

    if (disk_set_builder)
    {
        auto keys = compute_disk_set_keys(holder.key_columns, key_sizes, rows, null_map);
        if (!keys->empty())
            disk_set_builder->add(std::move(keys));
        return true;
    }

    data->callOnMethod([&](auto & method)
    {
        insertFromBlockImpl(method, holder.key_columns, rows, *data, null_map, holder.filter ? &holder.filter->getData() : nullptr);
    });

    bool within_limits = limits.check(data->getTotalRowCount(), data->getTotalByteCount(), "IN-set", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    if (!within_limits)
        is_truncated = true;
    /// Actual allocations and concurrent operators can consume more memory than projected before insertion.
    else if (isSpillNeeded())
        spillWhileBuilding("query memory exceeds the spill threshold after insertion");
    return within_limits;
}

void Set::appendSetElements(SetKeyColumns & holder)
{
    if (holder.key_columns.size() != keys_size || set_elements.size() != keys_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of key columns for set. Expected {} got {} and {}",
                        keys_size, holder.key_columns.size(), set_elements.size());

    size_t rows = holder.key_columns.at(0)->size();
    for (size_t i = 0; i < keys_size; ++i)
    {
        auto filtered_column = holder.key_columns[i]->filter(holder.filter->getData(), rows);
        if (set_elements[i]->empty())
            set_elements[i] = IColumn::mutate(std::move(filtered_column));
        else
            set_elements[i]->insertRangeFrom(*filtered_column, 0, filtered_column->size());
        if (transform_null_in && holder.null_map_holder)
            set_elements[i]->insert(Null{});
    }
}

void Set::checkIsCreated() const
{
    if (!is_created.load())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to use set before it has been built.");
}

size_t Set::getSetElementsBytes() const
{
    size_t bytes = 0;
    for (const auto & column : set_elements)
        bytes += column->byteSize();
    return bytes;
}

std::shared_ptr<const PlainRanges> Set::getPlainRanges() const
{
    /// The result is cached on first use, so reading `set_elements` before the set is filled would not
    /// merely give one wrong answer - it would pin an empty range set for every later caller.
    checkIsCreated();

    callOnce(
        plain_ranges_once,
        [this]
        {
            /// A tuple set has no single-column range representation; leave the cache null.
            if (set_elements.size() != 1)
                return;

            const auto & column = *set_elements.front();
            Ranges ranges;
            ranges.reserve(column.size());
            for (size_t i = 0; i < column.size(); ++i)
                ranges.emplace_back(column[i]);

            plain_ranges = std::make_shared<const PlainRanges>(ranges, /*may_have_intersection*/ true, /*ordered*/ false);
        });

    return plain_ranges;
}

Columns Set::getSetElements() const
{
    checkIsCreated();
    Columns result;
    result.reserve(set_elements.size());
    for (const auto & col : set_elements)
        result.push_back(col->getPtr());
    return result;
}

static ColumnUInt8::Ptr checkDateTimePrecision(const ColumnWithTypeAndName & column_to_cast)
{
    // Handle nullable columns
    const ColumnNullable * original_nullable_column = typeid_cast<const ColumnNullable *>(column_to_cast.column.get());
    const IColumn * original_nested_column = original_nullable_column
        ? &original_nullable_column->getNestedColumn()
        : column_to_cast.column.get();

    // Check if the original column is of ColumnDecimal<DateTime64> type
    const auto * original_decimal_column = typeid_cast<const ColumnDecimal<DateTime64> *>(original_nested_column);
    if (!original_decimal_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnDecimal for DateTime64");

    // Get the data array from the original column
    const auto & original_data = original_decimal_column->getData();
    size_t vec_res_size = original_data.size();

    // Prepare the precision null map
    auto precision_null_map_column = ColumnUInt8::create(vec_res_size, static_cast<UInt8>(0));
    NullMap & precision_null_map = precision_null_map_column->getData();

    // Determine which rows should be null based on precision loss
    const auto * datetime64_type = assert_cast<const DataTypeDateTime64 *>(column_to_cast.type.get());
    auto scale = datetime64_type->getScale();
    if (scale >= 1)
    {
        Int64 scale_multiplier = common::exp10_i32(scale);
        for (size_t row = 0; row < vec_res_size; ++row)
        {
            Int64 value = original_data[row];
            if (value % scale_multiplier != 0)
                precision_null_map[row] = 1; // Mark as null due to precision loss
            else
                precision_null_map[row] = 0;
        }
    }

    return precision_null_map_column;
}

static ColumnPtr mergeNullMaps(const ColumnPtr & null_map_column1, const ColumnUInt8::Ptr & null_map_column2)
{
    if (!null_map_column1)
        return null_map_column2;
    if (!null_map_column2)
        return null_map_column1;

    const auto & null_map1 = assert_cast<const ColumnUInt8 &>(*null_map_column1).getData();
    const auto & null_map2 = (*null_map_column2).getData();

    size_t size = null_map1.size();
    if (size != null_map2.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Null maps have different sizes");

    auto merged_null_map_column = ColumnUInt8::create(size);
    auto & merged_null_map = merged_null_map_column->getData();

    for (size_t i = 0; i < size; ++i)
        merged_null_map[i] = null_map1[i] || null_map2[i];

    return merged_null_map_column;
}

void Set::processDateTime64Column(
    const ColumnWithTypeAndName & column_to_cast,
    ColumnPtr & result,
    ColumnPtr & null_map_holder,
    ConstNullMapPtr & null_map) const
{
    // Check for sub-second precision and create a null map
    ColumnUInt8::Ptr filtered_null_map_column = checkDateTimePrecision(column_to_cast);

    // Extract existing null map and nested column from the result
    const ColumnNullable * result_nullable_column = typeid_cast<const ColumnNullable *>(result.get());
    const IColumn * nested_result_column = result_nullable_column
        ? &result_nullable_column->getNestedColumn()
        : result.get();

    ColumnPtr existing_null_map_column = result_nullable_column
        ? result_nullable_column->getNullMapColumnPtr()
        : nullptr;

    if (transform_null_in)
    {
        if (!null_map_holder)
            null_map_holder = filtered_null_map_column;
        else
            null_map_holder = mergeNullMaps(null_map_holder, filtered_null_map_column);

        const ColumnUInt8 * null_map_column = checkAndGetColumn<ColumnUInt8>(null_map_holder.get());
        if (!null_map_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Null map must be ColumnUInt8");

        null_map = &null_map_column->getData();
    }
    else
    {
        ColumnPtr merged_null_map_column = mergeNullMaps(existing_null_map_column, filtered_null_map_column);
        result = ColumnNullable::create(nested_result_column->getPtr(), merged_null_map_column);
    }
}

ColumnPtr Set::execute(const ColumnsWithTypeAndName & columns, bool negative) const
{
    size_t num_key_columns = columns.size();

    if (0 == num_key_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No columns passed to Set::execute method.");

    auto res = ColumnUInt8::create();
    ColumnUInt8::Container & vec_res = res->getData();
    vec_res.resize(columns.at(0).column->size());

    if (vec_res.empty())
        return res;

    std::shared_lock lock(rwlock);

    /// If the set is empty.
    if (data_types.empty())
    {
        if (negative)
            memset(vec_res.data(), 1, vec_res.size());
        else
            memset(vec_res.data(), 0, vec_res.size());
        return res;
    }

    checkColumnsNumber(num_key_columns);

    /// Remember the columns we will work with. Also check that the data types are correct.
    ColumnRawPtrs key_columns;
    key_columns.reserve(num_key_columns);

    /// The constant columns to the left of IN are not supported directly. For this, they first materialize.
    Columns materialized_columns;
    materialized_columns.reserve(num_key_columns);

    /// We will check existence in Set only for keys whose components do not contain any NULL value.
    ConstNullMapPtr null_map{};
    ColumnPtr null_map_holder;

    for (size_t i = 0; i < num_key_columns; ++i)
    {
        ColumnPtr result;

        const auto & column_before_cast = columns.at(i);
        ColumnWithTypeAndName column_to_cast
            = {column_before_cast.column->convertToFullColumnIfConst(), column_before_cast.type, column_before_cast.name};

        /// Since we have optional support for Nullable(Tuple), if `data_types[i]` is `Tuple(...)` type, then
        /// we will enter the `castColumnAccurateOrNull` path; however, it can lead to casted column type
        /// becomes `Tuple(Nullable(...), Nullable(...))` which will create problems during matching keys in Set.
        /// To avoid that, we do not do `castColumnAccurateOrNull` for Tuple types.
        auto target_type_without_nullable = removeNullable(data_types[i]);
        bool is_tuple_type = typeid_cast<const DataTypeTuple *>(target_type_without_nullable.get()) != nullptr;

        bool use_cast_accurate_or_null = !transform_null_in && data_types[i]->canBeInsideNullable() && !is_tuple_type;
        /// The same restrictions, for the lenient conversion below: it also produces a `Nullable`.
        /// When the probe already has the key type (up to `Nullable`), no value can fail to fit, so the
        /// cheap paths below are kept: an exact-type cast is a no-op, and that is the common
        /// `x IN set_table` case.
        bool can_convert_leniently = target_type_without_nullable->canBeInsideNullable() && !is_tuple_type
            && !removeNullable(column_to_cast.type)->equals(*target_type_without_nullable);

        if (use_cast_accurate_or_null)
        {
            result = castColumnAccurateOrNull(column_to_cast, data_types[i], cast_cache.get());
        }
        else
        {
            /// Special case when transform_null_in = true and type of column is Nullable but type of this key in Set is not Nullable.
            /// For example: SELECT NULL::Nullable(String) IN (SELECT 'abc') SETTINGS transform_null_in = 1;
            /// In this case we cannot just cast Nullable column to non-nullable type because it will fail if column contains nulls.
            /// We should cast nested column and remember the null map to use negative value on rows with null (as key column is not
            /// Nullable, Set cannot contain nulls for this column anyhow).
            /// Marks rows that cannot be members of the set for this key column, so they are skipped
            /// instead of being compared.
            auto add_non_member_rows = [&](ColumnPtr mask)
            {
                if (!null_map_holder)
                {
                    null_map_holder = std::move(mask);
                }
                else
                {
                    MutableColumnPtr mutable_null_map_holder = IColumn::mutate(std::move(null_map_holder));

                    PaddedPODArray<UInt8> & mutable_null_map = assert_cast<ColumnUInt8 &>(*mutable_null_map_holder).getData();
                    const PaddedPODArray<UInt8> & other_null_map = assert_cast<const ColumnUInt8 &>(*mask).getData();
                    for (size_t j = 0, size = mutable_null_map.size(); j < size; ++j)
                        mutable_null_map[j] |= other_null_map[j];

                    null_map_holder = std::move(mutable_null_map_holder);
                }

                null_map = &assert_cast<const ColumnUInt8 &>(*null_map_holder).getData();
            };

            if (can_convert_leniently)
            {
                /// `transform_null_in` makes NULL an ordinary matchable value, so the accurate-or-null
                /// cast above is not taken. A value the key type cannot represent is still simply not a
                /// member - which is what the literal `IN` list concludes, because it compares in a
                /// common supertype - but converting it strictly raised `CANNOT_CONVERT_TYPE` on rows
                /// the read path happened to deliver, so `v IN set_table` failed where `v IN (1)`
                /// returned 0.
                ///
                /// Convert leniently and mark the values that did not fit as non-members. This also
                /// covers a `Nullable` probe against a non-`Nullable` key, which `nullIn` and `IN` with
                /// `transform_null_in = 1` reach directly: there a strict cast of the nested column
                /// threw on the very same out-of-range values.
                auto casted = castColumnAccurateOrNull(column_to_cast, makeNullable(target_type_without_nullable), cast_cache.get());
                const auto & casted_nullable = assert_cast<const ColumnNullable &>(*casted);
                const size_t num_rows = casted_nullable.size();

                if (data_types[i]->isNullable())
                {
                    /// The set can hold a NULL, and a value that merely does not fit is not that NULL,
                    /// so the key column keeps the rows' original nullness and only the NULLs the cast
                    /// invented become non-members.
                    const auto * source_nullable = typeid_cast<const ColumnNullable *>(column_to_cast.column.get());

                    auto did_not_fit = ColumnUInt8::create(num_rows, UInt8(0));
                    auto & did_not_fit_data = did_not_fit->getData();
                    const auto & null_after_cast = casted_nullable.getNullMapData();
                    for (size_t row = 0; row < num_rows; ++row)
                        did_not_fit_data[row] = null_after_cast[row] && !(source_nullable && source_nullable->isNullAt(row));

                    ColumnPtr original_null_map = source_nullable
                        ? source_nullable->getNullMapColumnPtr()
                        : ColumnUInt8::create(num_rows, UInt8(0));
                    result = ColumnNullable::create(casted_nullable.getNestedColumnPtr(), original_null_map);
                    add_non_member_rows(std::move(did_not_fit));
                }
                else
                {
                    /// The key column is not `Nullable`, so the set has nothing to match a NULL against:
                    /// a source NULL and a value that does not fit are both non-members, and together
                    /// they are exactly the NULLs of the lenient cast.
                    result = casted_nullable.getNestedColumnPtr();
                    add_non_member_rows(casted_nullable.getNullMapColumnPtr());
                }
            }
            else if (transform_null_in && column_to_cast.type->isNullable() && !data_types[i]->isNullable())
            {
                /// A key type that cannot be lenient (a `Tuple`, or a type that cannot be inside
                /// `Nullable`) still needs the source NULLs kept away from the strict cast.
                auto nested_type = assert_cast<const DataTypeNullable &>(*column_to_cast.type).getNestedType();
                const auto & column_nullable = assert_cast<const ColumnNullable &>(*column_to_cast.column);
                result = castColumnAccurate(ColumnWithTypeAndName(column_nullable.getNestedColumnPtr(), nested_type, column_to_cast.name), data_types[i], cast_cache.get());
                add_non_member_rows(column_nullable.getNullMapColumnPtr());
            }
            else
            {
                result = castColumnAccurate(column_to_cast, data_types[i], cast_cache.get());
            }
        }

        // If the original column is DateTime64, check for sub-second precision
        if (isDateTime64(column_to_cast.column->getDataType()) && !isDateTime64(removeNullable(result)->getDataType()))
        {
            processDateTime64Column(column_to_cast, result, null_map_holder, null_map);
        }

        // Append the result to materialized columns
        materialized_columns.emplace_back(std::move(result));
        key_columns.emplace_back(materialized_columns.back().get());
    }

    if (!transform_null_in)
        null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

    if (disk_set)
    {
        executeDiskSet(key_columns, vec_res, negative, null_map);
        return res;
    }

    std::unique_ptr<CuckooLookup> new_cuckoo_lookup;
    const UInt64 version_of_keys = keys_version;
    /// The hash table skips the rows with a NULL key, while the table looks up every row, so it is used only if at
    /// most half of the rows are NULL, and only the other rows count towards building it.
    const bool may_use_cuckoo_lookup = cuckoo_lookup || rows_before_cuckoo_lookup.load(std::memory_order_relaxed) > 0;
    const size_t num_null_rows = null_map && may_use_cuckoo_lookup ? countBytesInFilter(*null_map) : 0;
    const bool used_cuckoo_lookup = cuckoo_lookup && num_null_rows * 2 <= vec_res.size() && data->callOnMethod([&](const auto & method)
        { return cuckoo_lookup->execute(method, key_columns, key_sizes, vec_res, negative, null_map); });
    if (!used_cuckoo_lookup)
    {
        data->callOnMethod([&](auto & method) { executeImpl(method, key_columns, vec_res, negative, vec_res.size(), null_map); });

        /// The keys do not change while the shared lock is held, so the table is built here and installed below.
        const Int64 rows = static_cast<Int64>(vec_res.size() - num_null_rows);
        if (rows > 0 && !cuckoo_lookup && rows_before_cuckoo_lookup.load(std::memory_order_relaxed) > 0)
        {
            const Int64 rows_before = rows_before_cuckoo_lookup.fetch_sub(rows, std::memory_order_relaxed);
            if (rows_before > 0 && rows_before <= rows)
                new_cuckoo_lookup = createCuckooLookup();
        }
    }

    /// The set is usually built before the rest of the query takes its memory, so it also spills while it is
    /// used. Spilling takes the lock exclusively, so this lookup releases its shared lock first.
    if (isSpillNeededAfterBuild())
    {
        /// The reason reads the table, which a concurrent spill can replace once the lock is released.
        const auto reason = fmt::format(
            "query memory with the {} of the set on disk exceeds the spill threshold",
            formatReadableSizeWithBinarySuffix(estimateMemoryOfDiskSet()));
        lock.unlock();
        spillAfterBuild(reason);
    }

    if (new_cuckoo_lookup)
    {
        if (lock.owns_lock())
            lock.unlock();
        std::lock_guard exclusive_lock(rwlock);
        /// Not if the set has spilled or its keys have changed meanwhile.
        if (keys_version == version_of_keys && data && !disk_set && !cuckoo_lookup)
            cuckoo_lookup = std::move(new_cuckoo_lookup);
    }

    return res;
}

bool Set::hasNull() const
{
    checkIsCreated();

    if (!transform_null_in)
        return false;

    if (data_types.size() != 1)
        return false;

    if (!data_types[0]->isNullable())
        return false;

    auto col = data_types[0]->createColumn();
    col->insert(Field());
    auto res = execute({ColumnWithTypeAndName(std::move(col), data_types[0], std::string())}, false);
    return res->getBool(0);
}

bool Set::empty() const
{
    std::shared_lock lock(rwlock);
    return data && data->empty();
}

size_t Set::getTotalRowCount() const
{
    std::shared_lock lock(rwlock);

    if (disk_set)
        return disk_set->getTotalRowCount();

    chassert(data);
    return data->getTotalRowCount();
}

size_t Set::getTotalByteCount() const
{
    std::shared_lock lock(rwlock);

    if (disk_set)
        return disk_set->getTotalByteCount();

    chassert(data);
    return data->getTotalByteCount();
}


template <typename Method>
void NO_INLINE Set::executeImpl(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    if (null_map)
        executeImplCase<Method, true>(method, key_columns, vec_res, negative, rows, null_map);
    else
        executeImplCase<Method, false>(method, key_columns, vec_res, negative, rows, null_map);
}


template <typename Method, bool has_null_map>
void NO_INLINE Set::executeImplCase(
    Method & method,
    const ColumnRawPtrs & key_columns,
    ColumnUInt8::Container & vec_res,
    bool negative,
    size_t rows,
    ConstNullMapPtr null_map) const
{
    Arena pool;
    typename Method::State state(key_columns, key_sizes, nullptr);

    /// Clustered key columns (e.g. a primary key prefix) arrive in runs of equal consecutive
    /// rows. The consecutive-keys optimization in ColumnsHashing handles them inside `findKey`:
    /// the last-element cache compares the key with the previous row's before probing the hash
    /// table, and `HashMethodHashed` additionally compares the raw key bytes before even
    /// calculating the hash.
    for (size_t i = 0; i < rows; ++i)
    {
        if (has_null_map && (*null_map)[i])
        {
            vec_res[i] = negative;
        }
        else
        {
            auto find_result = state.findKey(method.data, i, pool);
            vec_res[i] = negative ^ find_result.isFound();
        }
    }
}


void Set::checkColumnsNumber(size_t num_key_columns) const
{
    if (data_types.size() != num_key_columns)
    {
        throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                        "Number of columns in section IN doesn't match. {} at left, {} at right.",
                        num_key_columns, data_types.size());
    }
}

bool Set::areTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    /// Out-of-bound access can happen when same set expression built with different columns.
    /// Caller may call this method to make sure that the set is indeed the one they want
    /// without awaring data_types.size().
    if (set_type_idx >= data_types.size())
        return false;
    return removeNullable(recursiveRemoveLowCardinality(data_types[set_type_idx]))
        ->equals(*removeNullable(recursiveRemoveLowCardinality(other_type)));
}

void Set::checkTypesEqual(size_t set_type_idx, const DataTypePtr & other_type) const
{
    if (!this->areTypesEqual(set_type_idx, other_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Types of column {} in section IN don't match: "
                        "{} on the left, {} on the right", toString(set_type_idx + 1),
                        other_type->getName(), data_types[set_type_idx]->getName());
}

static UInt64 getNextMergeTreeSetIndexId()
{
    static std::atomic<UInt64> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

template <typename T, typename FieldValueRange>
static std::optional<BoolMask> checkInRangeSingleIntegerColumn(
    const IColumn & column, const FieldValueRange & range, bool all_keys)
{
    if (!range.left.isNormal() || !range.right.isNormal())
        return std::nullopt;

    const auto * typed_column = typeid_cast<const ColumnVector<T> *>(&column);
    if (!typed_column)
        return std::nullopt;

    const auto & values = typed_column->getData();
    const T left = static_cast<const ColumnVector<T> &>(*range.left.column).getData()[0];
    const T right = static_cast<const ColumnVector<T> &>(*range.right.column).getData()[0];
    /// Let the general path validate ranges that can produce inverted search positions.
    if (left > right || (left == right && !range.left_included && !range.right_included))
        return std::nullopt;

    size_t begin = 0;
    size_t length = values.size();
    while (length > 1)
    {
        const size_t half = length / 2;
        const T value = values[begin + half - 1];
        const bool below = value < left || (value == left && !range.left_included);
        begin = below ? begin + half : begin;
        length -= half;
    }
    if (length)
        begin += values[begin] < left || (values[begin] == left && !range.left_included);

    const bool can_be_true = begin < values.size()
        && (values[begin] < right || (values[begin] == right && range.right_included));
    return BoolMask{can_be_true, all_keys && left == right ? !can_be_true : true};
}

template <typename FieldValueRange>
static std::optional<BoolMask> checkInRangeSingleColumn(
    const IColumn & column, const FieldValueRange & range, bool all_keys)
{
    switch (column.getDataType())
    {
        case TypeIndex::UInt8: return checkInRangeSingleIntegerColumn<UInt8>(column, range, all_keys);
        case TypeIndex::UInt16: return checkInRangeSingleIntegerColumn<UInt16>(column, range, all_keys);
        case TypeIndex::UInt32: return checkInRangeSingleIntegerColumn<UInt32>(column, range, all_keys);
        case TypeIndex::UInt64: return checkInRangeSingleIntegerColumn<UInt64>(column, range, all_keys);
        case TypeIndex::Int8: return checkInRangeSingleIntegerColumn<Int8>(column, range, all_keys);
        case TypeIndex::Int16: return checkInRangeSingleIntegerColumn<Int16>(column, range, all_keys);
        case TypeIndex::Int32: return checkInRangeSingleIntegerColumn<Int32>(column, range, all_keys);
        case TypeIndex::Int64: return checkInRangeSingleIntegerColumn<Int64>(column, range, all_keys);
        default: return std::nullopt;
    }
}

MergeTreeSetIndex::MergeTreeSetIndex(const Columns & set_elements, std::vector<KeyTuplePositionMapping> && indexes_mapping_)
    : has_all_keys(set_elements.size() == indexes_mapping_.size())
    , indexes_mapping(std::move(indexes_mapping_))
    , instance_id(getNextMergeTreeSetIndexId())
{
    ::sort(indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return std::tie(l.key_index, l.tuple_index) < std::tie(r.key_index, r.tuple_index);
        });

    /// Deduplicate key columns. This can make the condition weaker, e.g.:
    ///     (x + 1, x + 2) IN (10, 10)
    /// effectively turns into:
    ///     (x + 1) IN (10)
    indexes_mapping.erase(std::unique(
        indexes_mapping.begin(), indexes_mapping.end(),
        [](const KeyTuplePositionMapping & l, const KeyTuplePositionMapping & r)
        {
            return l.key_index == r.key_index;
        }), indexes_mapping.end());

    size_t tuple_size = indexes_mapping.size();
    ordered_set.resize(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = set_elements[indexes_mapping[i].tuple_index];

    Block block_to_sort;
    SortDescription sort_description;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        String column_name = "_" + toString(i);
        block_to_sort.insert({ordered_set[i], nullptr, column_name});
        sort_description.emplace_back(column_name, 1, 1);
    }

    sortBlock(block_to_sort, sort_description);

    for (size_t i = 0; i < tuple_size; ++i)
        ordered_set[i] = block_to_sort.getByPosition(i).column;

    /// Only fixed-width key columns keep the cached ranges buffer small and bounded: their
    /// columns hold at most one value, so reuse cannot grow them. Variable-width columns
    /// (e.g. `ColumnString`) do not release reserved capacity on popBack, so caching them in
    /// thread-local storage could pin arbitrarily large buffers after the query ends.
    cache_ranges = std::all_of(ordered_set.begin(), ordered_set.end(),
        [](const ColumnPtr & column) { return column->valuesHaveFixedSize(); });
}

MergeTreeSetIndex::FieldValueRanges & MergeTreeSetIndex::getFieldValueRangesBuffer(FieldValueRanges & scratch) const
{
    size_t tuple_size = indexes_mapping.size();

    if (!cache_ranges)
    {
        /// Query-scoped scratch buffer: normally tracked, freed when the caller returns.
        scratch.reserve(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
            scratch.emplace_back(*ordered_set[i]);
        return scratch;
    }

    struct CacheEntry
    {
        UInt64 set_index_id;
        FieldValueRanges ranges;
    };
    /// Must exceed the number of IN-set key conditions checked per mark within one query,
    /// otherwise every call misses and rebuilds the buffer as before this cache existed.
    static constexpr size_t max_entries = 8;
    thread_local std::vector<CacheEntry> cache;

    for (auto & entry : cache)
        if (entry.set_index_id == instance_id)
            return entry.ranges;

    /// The cache outlives the query, so its memory (also freed here on eviction) must not be
    /// charged to whichever query happens to be running on this thread. See FieldValue::update
    /// for the same blocker on reallocations of the cached columns.
    MemoryTrackerBlockerInThread blocker;

    FieldValueRanges ranges;
    ranges.reserve(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i)
        ranges.emplace_back(*ordered_set[i], /*block_memory_tracker=*/ true);

    if (cache.size() >= max_entries)
        cache.erase(cache.begin());
    cache.push_back({instance_id, std::move(ranges)});
    return cache.back().ranges;
}

/** Return the BoolMask where:
  * 1: the intersection of the set and the range is non-empty
  * 2: the range contains elements not in the set
  */
BoolMask MergeTreeSetIndex::checkInRange(
    const std::vector<int> & key_col_to_sparse_pos,
    const Ranges & sparse_key_ranges,
    const DataTypes & sparse_data_types,
    size_t first_function_number,
    bool single_point) const
{
    auto get_sparse_info = [&](size_t key_column) -> std::pair<bool, size_t>
    {
        bool is_key_col_present = (key_column < key_col_to_sparse_pos.size() && key_col_to_sparse_pos[key_column] != -1);
        const size_t sparse_pos = is_key_col_present ? static_cast<size_t>(key_col_to_sparse_pos[key_column]) : 0;
        return {is_key_col_present, sparse_pos};
    };

    size_t tuple_size = indexes_mapping.size();

    FieldValueRanges scratch;
    FieldValueRanges & ranges = getFieldValueRangesBuffer(scratch);
    size_t function_number = first_function_number;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        const size_t mapping_first_function_number = function_number;
        function_number += indexes_mapping[i].functions.size();

        size_t key_column = indexes_mapping[i].key_index;
        auto [is_key_col_present, sparse_pos] = get_sparse_info(key_column);

        FieldValueRange & range = ranges[i];

        if (!is_key_col_present)
        {
            /// We have no range information for this key column.
            /// Most likely earlier columns were high cardinality, so this column and later column marks were not loaded into memory
            /// Treat it as completely unconstrained; since we do not have the type information,
            /// we do not know whether to createWholeUniverse() or createWholeUniverseWithoutNull().
            /// So, we choose the more relaxed option:
            /// [-inf, +inf] instead of ( -inf, +inf ).
            range.left.update(NEGATIVE_INFINITY);
            range.right.update(POSITIVE_INFINITY);
            range.left_included = true;
            range.right_included = true;
            continue;
        }

        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            sparse_key_ranges[sparse_pos],
            indexes_mapping[i].functions,
            mapping_first_function_number,
            sparse_data_types[sparse_pos],
            single_point);

        if (!new_range)
            return {true, true};

        range.left.update(new_range->left);
        range.right.update(new_range->right);
        range.left_included = new_range->left_included;
        range.right_included = new_range->right_included;
    }

    if (tuple_size == 1)
        if (auto result = checkInRangeSingleColumn(*ordered_set[0], ranges[0], has_all_keys))
            return *result;

    /// lhs < rhs return -1
    /// lhs == rhs return 0
    /// lhs > rhs return 1
    auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
    {
        if (rhs.isNegativeInfinity())
            return +1;
        if (rhs.isPositiveInfinity())
            return lhs.isNullAt(row) ? 0 : -1; // +Inf == +Inf
        return lhs.compareAt(row, 0, *rhs.column, 1);
    };

    /// Because ordered_set is sorted lexicographically, the elements we're looking for are
    /// consecutive. Use binary search to find the range of indices.

    /// The part about left_included/right_included is a little tricky. It was initially implemented
    /// incorrectly:
    ///   begin = lower_bound(..., left_point, tuple_less_unaware_of_includedness);
    ///   if (!all(ranges[..].left_included) && equals(left_point, begin))
    ///       begin += 1;
    /// This breaks on the following example:
    ///   key_ranges = [(0, +inf), (-inf, +inf)]  (the 0 is not included),
    ///   ordered_set = [[0], [0]].
    /// The incorrect implementation would output begin == 0 and conclude that the set element is
    /// inside the range (it isn't). The `begin += 1` won't happen because (0, 0) != (0, -inf).

    auto indices = collections::range(0, size());
    size_t begin = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return true if set[row] is below the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].left, row);

                if (cmp > 0)
                    return false;
                /// Note: if some range has left_included == false then the left ends of all
                /// subsequent ranges' don't matter. (Symmetrically for right.)
                /// It's the only way to make sense of the notion of a range of tuples where the
                /// included/excluded flags are given per element.
                if (cmp < 0 || (cmp == 0 && !ranges[i].left_included))
                    return true;
            }
            return false;
        }) - indices.begin();
    size_t end = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return false if set[row] is above the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].right, row);

                if (cmp > 0 || (cmp == 0 && !ranges[i].right_included))
                    return false;
                if (cmp < 0)
                    return true;
            }
            return true;
        }) - indices.begin();

    if (begin > end)
    {
        /// No set element can lie in an empty input key range (e.g. the open interval between two adjacent integers).
        for (const auto & mapping : indexes_mapping)
        {
            auto [is_key_col_present, sparse_pos] = get_sparse_info(mapping.key_index);
            if (is_key_col_present && sparse_key_ranges[sparse_pos].empty())
                return {false, true};
        }

        /// TODO: Remove the #ifndef and always throw after
        ///       https://github.com/ClickHouse/ClickHouse/issues/90461 is fixed.
        ///       (What happens here is: the applyMonotonicFunctionsChainToRange call above applies
        ///        nonmonotonic functions, and we end up with left > right.)
#ifndef NDEBUG
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary search result in MergeTreeSetIndex");
#else
        return {true, true};
#endif
    }

    bool can_be_true = begin < end;

    /// A special case of 1-element KeyRange. It's useful for partition pruning.
    bool at_most_one_element_range = true;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto & r = ranges[i];
        if (r.left.isNormal() && r.right.isNormal())
        {
            if (0 != r.left.column->compareAt(0, 0, *r.right.column, 1))
            {
                at_most_one_element_range = false;
                break;
            }
        }
        else if ((r.left.isPositiveInfinity() && r.right.isPositiveInfinity()) || (r.left.isNegativeInfinity() && r.right.isNegativeInfinity()))
        {
            /// Special value equality.
        }
        else
        {
            at_most_one_element_range = false;
            break;
        }
    }
    if (at_most_one_element_range && has_all_keys)
    {
        /// Here we know that there is at most one element in range.
        /// The main difference with the normal case is that we can definitely say that
        /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).
        return {can_be_true, !can_be_true};
    }

    return {can_be_true, true};
}

BoolMask MergeTreeSetIndex::checkInRange(const Ranges & key_ranges, const DataTypes & data_types, size_t first_function_number, bool single_point) const
{
    size_t tuple_size = indexes_mapping.size();

    FieldValueRanges scratch;
    FieldValueRanges & ranges = getFieldValueRangesBuffer(scratch);
    size_t function_number = first_function_number;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        const size_t mapping_first_function_number = function_number;
        function_number += indexes_mapping[i].functions.size();

        if (indexes_mapping[i].key_index >= key_ranges.size())
            return {true, true};

        std::optional<Range> new_range = KeyCondition::applyMonotonicFunctionsChainToRange(
            key_ranges[indexes_mapping[i].key_index],
            indexes_mapping[i].functions,
            mapping_first_function_number,
            data_types[indexes_mapping[i].key_index],
            single_point);

        if (!new_range)
            return {true, true};

        FieldValueRange & range = ranges[i];
        range.left.update(new_range->left);
        range.right.update(new_range->right);
        range.left_included = new_range->left_included;
        range.right_included = new_range->right_included;
    }

    if (tuple_size == 1)
        if (auto result = checkInRangeSingleColumn(*ordered_set[0], ranges[0], has_all_keys))
            return *result;

    /// lhs < rhs return -1
    /// lhs == rhs return 0
    /// lhs > rhs return 1
    auto compare = [](const IColumn & lhs, const FieldValue & rhs, size_t row)
    {
        if (rhs.isNegativeInfinity())
            return +1;
        if (rhs.isPositiveInfinity())
            return lhs.isNullAt(row) ? 0 : -1; // +Inf == +Inf
        return lhs.compareAt(row, 0, *rhs.column, 1);
    };

    /// Because ordered_set is sorted lexicographically, the elements we're looking for are
    /// consecutive. Use binary search to find the range of indices.

    /// The part about left_included/right_included is a little tricky. It was initially implemented
    /// incorrectly:
    ///   begin = lower_bound(..., left_point, tuple_less_unaware_of_includedness);
    ///   if (!all(ranges[..].left_included) && equals(left_point, begin))
    ///       begin += 1;
    /// This breaks on the following example:
    ///   key_ranges = [(0, +inf), (-inf, +inf)]  (the 0 is not included),
    ///   ordered_set = [[0], [0]].
    /// The incorrect implementation would output begin == 0 and conclude that the set element is
    /// inside the range (it isn't). The `begin += 1` won't happen because (0, 0) != (0, -inf).

    auto indices = collections::range(0, size());
    size_t begin = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return true if set[row] is below the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].left, row);

                if (cmp > 0)
                    return false;
                /// Note: if some range has left_included == false then the left ends of all
                /// subsequent ranges' don't matter. (Symmetrically for right.)
                /// It's the only way to make sense of the notion of a range of tuples where the
                /// included/excluded flags are given per element.
                if (cmp < 0 || (cmp == 0 && !ranges[i].left_included))
                    return true;
            }
            return false;
        }) - indices.begin();
    size_t end = std::partition_point(indices.begin(), indices.end(), [&](size_t row)
        {
            /// Return false if set[row] is above the key range.
            for (size_t i = 0; i < tuple_size; ++i)
            {
                int cmp = compare(*ordered_set[i], ranges[i].right, row);

                if (cmp > 0 || (cmp == 0 && !ranges[i].right_included))
                    return false;
                if (cmp < 0)
                    return true;
            }
            return true;
        }) - indices.begin();

    if (begin > end)
    {
        /// No set element can lie in an empty input key range (e.g. the open interval between two adjacent integers).
        for (const auto & mapping : indexes_mapping)
        {
            if (key_ranges[mapping.key_index].empty())
                return {false, true};
        }

        /// TODO: Remove the #ifndef and always throw after
        ///       https://github.com/ClickHouse/ClickHouse/issues/90461 is fixed.
        ///       (What happens here is: the applyMonotonicFunctionsChainToRange call above applies
        ///        nonmonotonic functions, and we end up with left > right.)
#ifndef NDEBUG
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid binary search result in MergeTreeSetIndex");
#else
        return {true, true};
#endif
    }

    bool can_be_true = begin < end;

    /// A special case of 1-element KeyRange. It's useful for partition pruning.
    bool at_most_one_element_range = true;
    for (size_t i = 0; i < tuple_size; ++i)
    {
        auto & r = ranges[i];
        if (r.left.isNormal() && r.right.isNormal())
        {
            if (0 != r.left.column->compareAt(0, 0, *r.right.column, 1))
            {
                at_most_one_element_range = false;
                break;
            }
        }
        else if ((r.left.isPositiveInfinity() && r.right.isPositiveInfinity()) || (r.left.isNegativeInfinity() && r.right.isNegativeInfinity()))
        {
            /// Special value equality.
        }
        else
        {
            at_most_one_element_range = false;
            break;
        }
    }
    if (at_most_one_element_range && has_all_keys)
    {
        /// Here we know that there is at most one element in range.
        /// The main difference with the normal case is that we can definitely say that
        /// condition in this range is always TRUE (can_be_false = 0) or always FALSE (can_be_true = 0).
        return {can_be_true, !can_be_true};
    }

    return {can_be_true, true};
}

bool MergeTreeSetIndex::hasMonotonicFunctionsChain() const
{
    for (const auto & mapping : indexes_mapping)
        if (!mapping.functions.empty())
            return true;
    return false;
}

void MergeTreeSetIndex::FieldValue::update(const Field & x)
{
    if (x.isNegativeInfinity() || x.isPositiveInfinity())
        value = x;
    else
    {
        /// When the column belongs to the thread-local buffer of getFieldValueRangesBuffer, which
        /// outlives the query, its reallocations must not be charged to the current query.
        std::optional<MemoryTrackerBlockerInThread> blocker;
        if (block_memory_tracker)
            blocker.emplace();
        /// Keep at most one element in column.
        if (!column->empty())
            column->popBack(1);
        column->insert(x);
        value = Field(); // Set back to normal value.
    }
}

}
