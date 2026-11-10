#include <Interpreters/HashJoin/HashJoinTable.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/joinDispatch.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <base/scope_guard.h>

#include <limits>
#include <numeric>

namespace ProfileEvents
{
extern const Event HashJoinPreallocatedElementsInHashTables;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

namespace
{

HashJoin::Type toTwoLevelType(HashJoin::Type type)
{
    using Type = HashJoin::Type;

    switch (type)
    {
        case Type::key8: return Type::two_level_key8;
        case Type::key16: return Type::two_level_key16;
        case Type::key32: return Type::two_level_key32;
        case Type::key64: return Type::two_level_key64;
        case Type::key_string: return Type::two_level_key_string;
        case Type::key_fixed_string: return Type::two_level_key_fixed_string;
        case Type::keys32: return Type::two_level_keys32;
        case Type::keys64: return Type::two_level_keys64;
        case Type::keys128: return Type::two_level_keys128;
        case Type::keys256: return Type::two_level_keys256;
        case Type::hashed: return Type::two_level_hashed;
        case Type::key_packed_string: return Type::two_level_key_packed_string;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash join map type {} has no two-level variant", type);
    }
}

}

HashJoinTable::HashJoinTable(
    Type key_class,
    Sizes key_sizes_,
    JoinKind kind,
    JoinStrictness strictness,
    JoinMapsKind maps_kind,
    const HashJoinBuildLayout & layout,
    bool robin_hood_allowed)
    : key_type(layout.two_level ? toTwoLevelType(key_class) : key_class)
    , key_sizes(std::move(key_sizes_))
    , slots(layout.num_slots)
{
    joinDispatchInit(kind, strictness, maps, maps_kind);
    std::visit([&](auto & maps_) { maps_.create(key_type, robin_hood_allowed); }, maps);

    arenas.reserve(layout.num_build_streams);
    for (size_t stream = 0; stream < layout.num_build_streams; ++stream)
        arenas.push_back(std::make_unique<Arena>());
}

size_t HashJoinTable::arenaBytes() const
{
    size_t res = 0;
    for (const auto & arena : arenas)
        if (arena)
            res += arena->allocatedBytes();
    return res;
}

std::vector<std::unique_ptr<Arena>> HashJoinTable::releaseArenas()
{
    /// An arena allocates its first chunk on its first use, so the arena of a stream that inserted nothing holds nothing.
    std::vector<std::unique_ptr<Arena>> res;
    for (auto & arena : arenas)
        if (arena && arena->allocatedBytes() != 0)
            res.push_back(std::move(arena));
    return res;
}

BuildResult HashJoinTable::insertSlots(
    const InsertFunctions & functions,
    JoinBuild::InsertKeysArgs args,
    std::span<const ScatteredBlock::Selector> per_slot,
    BuildResult result,
    absl::FunctionRef<void(size_t)> add_bytes)
{
    /// No other call allocates in the arena meanwhile. The guard also counts what was allocated before a throw, as the slots do below.
    const size_t arena_bytes_before = args.pool.allocatedBytes();
    SCOPE_EXIT({ add_bytes(args.pool.allocatedBytes() - arena_bytes_before); });

    auto insert_slot = [&](size_t slot)
    {
        const ScatteredBlock::Selector & selector = per_slot[slot];
        const size_t bytes_before = functions.slot_bytes(slots, args.map, slot);
        /// Under the slot lock so a concurrent limit check cannot miss this slot. Also when an insert throws: the
        /// rows before it stay in the map, and `StorageJoin` keeps serving them.
        SCOPE_EXIT({ add_bytes(functions.slot_bytes(slots, args.map, slot) - bytes_before); });

        /// Checked here, so that a slot with nothing to reserve skips the call.
        if (const size_t reserved = slots.mayReserve(slot) ? functions.reserve_on_first_insert(slots, args.map, slot) : 0)
            ProfileEvents::increment(ProfileEvents::HashJoinPreallocatedElementsInHashTables, reserved);

        const bool row_list = !selector.isContinuousRange();
        if (row_list)
        {
            const auto & indexes = selector.getIndexes().getData();
            args.rows = {.rows = indexes.data(), .size = indexes.size()};
        }
        else
        {
            const auto range = selector.getRange();
            args.rows = {.first = range.first, .size = range.second - range.first};
        }
        const BuildResult slot_result = functions.insert_keys[row_list](args);

        result.is_inserted |= slot_result.is_inserted;
        result.all_values_unique &= slot_result.all_values_unique;
    };

    slots.insert(per_slot, insert_slot);
    return result;
}

void HashJoinTable::throwKeysNotRecoverable(Type type)
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_JOIN_KEYS,
        "Cannot read a Join table whose keys are stored as {}: the key values are not recoverable "
        "from the map. Read it with a JOIN or joinGet instead",
        type);
}

namespace detail
{

std::vector<size_t> packedKeyOrder(const Sizes & clause_sizes, const std::optional<Sizes> & packed_sizes)
{
    std::vector<size_t> order(clause_sizes.size());
    if (!packed_sizes)
    {
        std::iota(order.begin(), order.end(), 0);
        return order;
    }

    if (packed_sizes->size() != clause_sizes.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "StorageJoin packed key has {} components but the join has {} keys",
            packed_sizes->size(),
            clause_sizes.size());

    std::vector<bool> taken(clause_sizes.size(), false);
    for (size_t position = 0; position < packed_sizes->size(); ++position)
    {
        auto slot = clause_sizes.size();
        for (size_t i = 0; i < clause_sizes.size(); ++i)
        {
            if (!taken[i] && clause_sizes[i] == (*packed_sizes)[position])
            {
                slot = i;
                break;
            }
        }
        if (slot == clause_sizes.size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "StorageJoin has no key of size {} left to pack", (*packed_sizes)[position]);
        taken[slot] = true;
        order[position] = slot;
    }
    return order;
}

void insertKey(MutableColumns & columns, const KeyLayout & layout, std::string_view key, size_t times)
{
    for (size_t i = 0; i < times; ++i)
    {
        if (layout.whole_key_pos)
            columns[*layout.whole_key_pos]->insertData(key.data(), key.size());
        else
            for (const auto & column : layout.packed)
                columns[column.output_pos]->insertData(key.data() + column.offset, column.width);
    }
}

}

void HashJoinTable::freezeForProbing()
{
    auto finish_fill = []<typename Table>(const std::shared_ptr<Table> & table)
    {
        if (!table)
            return;
        table->computeBucketPrefix();
        if constexpr (is_partitioned_fixed_table<Table>)
            table->restoreMinMaxOptimization();
    };
    std::visit([&](auto & maps_) { maps_.visit(key_type, finish_fill); }, maps);
}

size_t HashJoinTable::bufferBytes() const
{
    return std::visit([&](const auto & maps_) { return maps_.getTotalByteCountImpl(key_type); }, maps);
}

size_t HashJoinTable::bufferSizeInCells() const
{
    return std::visit([&](const auto & maps_) { return maps_.getBufferSizeInCells(key_type); }, maps);
}

size_t HashJoinTable::cellSize() const
{
    return std::visit(
        [&](const auto & maps_)
        { return maps_.visit(key_type, []<typename Table>(const std::shared_ptr<Table> &) { return sizeof(typename Table::cell_type); }); },
        maps);
}

template <bool is_signed, typename Key, typename Maps>
bool HashJoinTable::tryConvertToFixedHashMapImpl(Maps & maps_)
{
    using SignedKey = std::make_signed_t<Key>;

    static constexpr size_t MAX_RANGE = (1ULL << 18);
    /// Limits conversion to cases where FixedHashMaps larger than 2^16 have at least 25% fill factor,
    /// ensuring they use at most around twice the memory of the source HashMap.
    static constexpr size_t MAX_RANGE_SPARSITY_FACTOR = 4;

    constexpr bool is_32 = std::is_same_v<Key, UInt32>;
    constexpr Type source_type = is_32 ? Type::key32 : Type::key64;
    auto & source_map = *maps_.template mapOf<source_type>();

    if (source_map.empty() || source_map.size() > MAX_RANGE)
        return false;

    size_t key_count = source_map.size();
    auto it = source_map.begin();
    Key min_key = it->getKey();
    Key max_key = it->getKey();
    ++it;

    /// Keys are stored as unsigned (UInt32/UInt64) in the hash map, but the original column
    /// may be signed (Int32/Int64). We must compare using signed arithmetic to find the true
    /// min/max
    for (; it != source_map.end(); ++it)
    {
        Key k = it->getKey();
        if constexpr (is_signed)
        {
            SignedKey signed_key = static_cast<SignedKey>(k);
            if (signed_key < static_cast<SignedKey>(min_key))
                min_key = k;
            if (signed_key > static_cast<SignedKey>(max_key))
                max_key = k;
        }
        else
        {
            if (k < min_key)
                min_key = k;
            if (k > max_key)
                max_key = k;
        }

        if (static_cast<size_t>(max_key - min_key) >= MAX_RANGE)
            return false;
    }

    size_t range = static_cast<size_t>(max_key - min_key) + 1;

    using Mapped = typename Maps::MappedType;
    auto convert_to_fixed_hash_map = [&]<size_t size_bits, Type range_type>()
    {
        using RangeMap = HashJoin::JoinFixedHashMapWithSizeBits<Key, Mapped, size_bits>;
        auto range_map = std::make_shared<RangeMap>();
        for (auto source_map_it = source_map.begin(); source_map_it != source_map.end(); ++source_map_it)
        {
            typename RangeMap::LookupResult res;
            bool inserted = false;
            range_map->emplace(source_map_it->getKey() - min_key, res, inserted);
            if constexpr (Maps::has_mapped)
            {
                if (inserted)
                    res->getMapped() = source_map_it->getMapped();
            }
        }
        maps_.template mapOf<range_type>() = std::move(range_map);
        key_type = range_type;
        key_range = {min_key, range};
        maps_.template mapOf<source_type>().reset();
        return true;
    };

    if (range <= (1ULL << 8))
        return convert_to_fixed_hash_map.template operator()<8, is_32 ? Type::range8_key32 : Type::range8_key64>();
    if (range <= (1ULL << 16))
        return convert_to_fixed_hash_map.template operator()<16, is_32 ? Type::range16_key32 : Type::range16_key64>();
    if (range <= (1ULL << 17))
        return (1ULL << 17) <= key_count * MAX_RANGE_SPARSITY_FACTOR
            && convert_to_fixed_hash_map.template operator()<17, is_32 ? Type::range17_key32 : Type::range17_key64>();
    return (1ULL << 18) <= key_count * MAX_RANGE_SPARSITY_FACTOR
        && convert_to_fixed_hash_map.template operator()<18, is_32 ? Type::range18_key32 : Type::range18_key64>();
}

bool HashJoinTable::tryConvertToFixedHashMap(bool is_signed)
{
    return std::visit(
        [&](auto & maps_)
        {
            using Maps = std::decay_t<decltype(maps_)>;
            if constexpr (std::is_same_v<Maps, HashJoin::MapsAsof>)
                return false;
            else if (key_type == Type::key32)
                return is_signed ? tryConvertToFixedHashMapImpl<true, UInt32>(maps_) : tryConvertToFixedHashMapImpl<false, UInt32>(maps_);
            else
                return is_signed ? tryConvertToFixedHashMapImpl<true, UInt64>(maps_) : tryConvertToFixedHashMapImpl<false, UInt64>(maps_);
        },
        maps);
}

namespace
{

/// Defensive check that T (= common_type) can hold every value of BuildKey.
/// `getLeastSupertype` guarantees this in normal flow; this catches contract violations
/// from future code changes and falls through to pass-all instead of misfiring bounds checks.
template <typename BuildKey, typename T>
constexpr bool canLosslesslyHold()
{
    if constexpr (std::is_unsigned_v<T> && std::is_signed_v<BuildKey>)
        return false;
    else if constexpr (std::is_signed_v<T> == std::is_signed_v<BuildKey>)
        return sizeof(T) >= sizeof(BuildKey);
    else /* T signed, BuildKey unsigned */
        return sizeof(T) > sizeof(BuildKey);
}

/// For each probe value of the column, returns 1 if it is a key of `ht`, else 0. A value outside the range of `BuildKey` is not a key.
/// Any other value is narrowed to `BuildKey`, reinterpreted as the unsigned key of the `FixedHashMap`, and looked up.
/// The loop is written twice, with and without the null mask, so that each copy stays branchless and vectorizable.
/// Probe types that cannot hold the range of `BuildKey` are filtered out at compile time. At runtime, a column of such a type
/// or a column that is not a `ColumnVector` is conservatively passed through (all 1).
template <typename BuildKey, typename HashMapT>
ColumnPtr probeFixedHashMap(
    const HashMapT & ht,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size,
    const ColumnWithTypeAndName & values)
{
    using UnsignedBK = std::make_unsigned_t<BuildKey>;

    const IColumn * col = values.column.get();
    const ColumnUInt8 * nm_col = nullptr;
    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(col))
    {
        col = &nullable->getNestedColumn();
        nm_col = &nullable->getNullMapColumn();
    }

    const size_t n = col->size();
    auto result_col = ColumnUInt8::create(n);
    UInt8 * result = result_col->getData().data();
    const UInt8 * null_map = nm_col ? nm_col->getData().data() : nullptr;

    const bool dispatched = castTypeToEither<
        ColumnVector<UInt8>, ColumnVector<UInt16>, ColumnVector<UInt32>, ColumnVector<UInt64>,
        ColumnVector<Int8>, ColumnVector<Int16>, ColumnVector<Int32>, ColumnVector<Int64>>(
        col,
        [&](const auto & typed_col) -> bool
        {
            using T = typename std::decay_t<decltype(typed_col)>::ValueType;
            if constexpr (canLosslesslyHold<BuildKey, T>())
            {
                constexpr T t_lo = static_cast<T>(std::numeric_limits<BuildKey>::min());
                constexpr T t_hi = static_cast<T>(std::numeric_limits<BuildKey>::max());
                const T * src = typed_col.getData().data();

                auto probe_one = [&](size_t i) -> UInt8
                {
                    const T v = src[i];
                    if (v < t_lo || v > t_hi)
                        return 0;
                    const UnsignedBK idx = static_cast<UnsignedBK>(static_cast<BuildKey>(v)) - min_key;
                    return (idx < range_size && ht.has(idx)) ? 1 : 0;
                };

                if (null_map)
                {
                    for (size_t i = 0; i < n; ++i)
                        result[i] = probe_one(i) & static_cast<UInt8>(!null_map[i]);
                }
                else
                {
                    for (size_t i = 0; i < n; ++i)
                        result[i] = probe_one(i);
                }
                return true;
            }
            else
            {
                return false;
            }
        });

    if (!dispatched)
        std::fill_n(result, n, UInt8(1));

    return result_col;
}

}

SharedFixedHashTableRuntimeFilter::ProbeFn HashJoinTable::makeSharedRuntimeFilter(bool build_signed) const
{
    SharedFixedHashTableRuntimeFilter::ProbeFn probe_fn;
    std::visit(
        [&](const auto & map)
        {
            using MapType = std::decay_t<decltype(map)>;
            if constexpr (
                std::is_same_v<MapType, HashJoin::MapsOne> || std::is_same_v<MapType, HashJoin::MapsAll>
                || std::is_same_v<MapType, HashJoin::MapsSet>)
            {
                /// The probe reads the map, so it owns a reference to it.
                auto dispatch = [&]<typename Signed, typename Unsigned>(const auto & range_ptr, UInt64 min_key, size_t range_size)
                {
                    if (!range_ptr)
                        return;
                    auto make_probe_fn = [&]<typename BuildKey>()
                    {
                        probe_fn = [range_map = range_ptr, min = static_cast<Unsigned>(min_key), range_size](
                                       const ColumnWithTypeAndName & values) -> ColumnPtr
                        { return probeFixedHashMap<BuildKey>(*range_map, min, range_size, values); };
                    };
                    if (build_signed)
                        make_probe_fn.template operator()<Signed>();
                    else
                        make_probe_fn.template operator()<Unsigned>();
                };

                /// For range_*_key_* the min/range come from `tryConvertToFixedHashMap`'s
                /// computed key_range; for key8/key16 the whole key space is the table so
                /// min=0 and range = 2^(Key bits).
                switch (key_type)
                {
                    case Type::key8:
                        dispatch.template operator()<Int8, UInt8>(map.key8, 0, 1ULL << 8);
                        break;
                    case Type::key16:
                        dispatch.template operator()<Int16, UInt16>(map.key16, 0, 1ULL << 16);
                        break;
                    case Type::range8_key32:
                        dispatch.template operator()<Int32, UInt32>(map.range8_key32, key_range.min_key, key_range.size);
                        break;
                    case Type::range16_key32:
                        dispatch.template operator()<Int32, UInt32>(map.range16_key32, key_range.min_key, key_range.size);
                        break;
                    case Type::range17_key32:
                        dispatch.template operator()<Int32, UInt32>(map.range17_key32, key_range.min_key, key_range.size);
                        break;
                    case Type::range18_key32:
                        dispatch.template operator()<Int32, UInt32>(map.range18_key32, key_range.min_key, key_range.size);
                        break;
                    case Type::range8_key64:
                        dispatch.template operator()<Int64, UInt64>(map.range8_key64, key_range.min_key, key_range.size);
                        break;
                    case Type::range16_key64:
                        dispatch.template operator()<Int64, UInt64>(map.range16_key64, key_range.min_key, key_range.size);
                        break;
                    case Type::range17_key64:
                        dispatch.template operator()<Int64, UInt64>(map.range17_key64, key_range.min_key, key_range.size);
                        break;
                    case Type::range18_key64:
                        dispatch.template operator()<Int64, UInt64>(map.range18_key64, key_range.min_key, key_range.size);
                        break;
                    default: break;
                }
            }
        },
        maps);
    return probe_fn;
}

namespace
{

/// Calls `f(map)` for each `ASOF` map of type `type` whose buckets have buffers of their own. That leaves out a
/// `PartitionedFixedHashTable`, which has one buffer for all its buckets, and a map with one bucket, which has nothing to split.
template <typename F>
void forEachAsofMapWithBucketBuffers(const HashJoin::MapsVariant & maps, HashJoin::Type type, F && f)
{
    if (const auto * asof_maps = std::get_if<HashJoin::MapsAsof>(&maps))
        asof_maps->visit(type, [&]<typename Table>(const std::shared_ptr<Table> & map)
        {
            if constexpr (!is_partitioned_fixed_table<Table> && Table::NUM_BUCKETS > 1)
                if (map)
                    f(*map);
        });
}

}

HashJoinTable::ParallelClearWork HashJoinTable::parallelClearWork() const
{
    ParallelClearWork work;
    forEachAsofMapWithBucketBuffers(maps, key_type, [&]<typename Table>(const Table & map)
    {
        work.buckets += Table::NUM_BUCKETS;
        work.keys += map.size();
    });
    return work;
}

void HashJoinTable::clearInParallel(size_t num_jobs, absl::FunctionRef<void(std::function<void()>)> schedule)
{
    forEachAsofMapWithBucketBuffers(maps, key_type, [&]<typename Table>(Table & table)
    {
        for (size_t first_bucket = 0; first_bucket < num_jobs; ++first_bucket)
            schedule([&table, first_bucket, num_jobs]
            {
                for (size_t bucket = first_bucket; bucket < Table::NUM_BUCKETS; bucket += num_jobs)
                    table.impls[bucket].clearAndShrink();
            });
    });
}

void HashJoinTable::releaseMaps()
{
    std::visit([](auto & maps_) { maps_ = std::decay_t<decltype(maps_)>(); }, maps);
}

}
