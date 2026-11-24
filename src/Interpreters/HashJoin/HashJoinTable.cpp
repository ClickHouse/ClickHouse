#include <Interpreters/HashJoin/HashJoinTable.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <base/sanitizer_defs.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/joinDispatch.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <base/getCPUCacheSize.h>
#include <base/getL2CacheSize.h>

#include <limits>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_JOIN_KEYS;
}

HashJoinTable::HashJoinTable(
    Type key_class,
    Sizes key_sizes_,
    JoinKind kind,
    JoinStrictness strictness,
    JoinMapsKind maps_kind,
    const HashJoinBuildLayout & layout,
    bool robin_hood_allowed_)
    : key_type(key_class)
    , key_sizes(std::move(key_sizes_))
    , robin_hood_allowed(robin_hood_allowed_)
{
    joinDispatchInit(kind, strictness, maps, maps_kind);
    std::visit([&](auto & maps_) { maps_.create(key_type); }, maps);

    arenas.reserve(layout.num_build_streams);
    for (size_t stream = 0; stream < layout.num_build_streams; ++stream)
        arenas.push_back(std::make_unique<Arena>());
    next_arena_chunks = std::make_unique<std::atomic<size_t>[]>(arenas.size());
    noteNextArenaChunks();
}

void HashJoinTable::usePartitions(size_t partition_bits)
{
    chassert(!isPartitioned() && keys() == 0);
    auto to_partitions = [&]<typename Table>(std::shared_ptr<Table> & table)
    {
        if constexpr (is_join_map_forms<Table>)
        {
            table->partitioned = std::make_unique<typename Table::partitioned_type>(partition_bits, robin_hood_allowed);
            table->partitioned->setReserveOnCreate(partition_reserve_keys);
            num_partitions = 1uz << partition_bits;
            table->single.reset();
        }
    };
    std::visit([&](auto & maps_) { maps_.visit(key_type, to_partitions); }, maps);
}

void HashJoinTable::addPartitionNonzeroCounts(std::span<size_t> counts) const
{
    chassert(counts.size() == numPartitions());
    auto count = [&]<typename Table>(const std::shared_ptr<Table> & table)
    {
        if constexpr (is_join_map_forms<Table>)
            for (size_t p = 0; p < counts.size(); ++p)
                if (const auto * map = table->partitioned->partitionOrNull(p))
                    counts[p] += map->size() - map->hasZero();
    };
    std::visit([&](const auto & maps_) { maps_.visit(key_type, count); }, maps);
}

void HashJoinTable::reserve(size_t keys_to_reserve)
{
    chassert(!isPartitioned());
    auto reserve_single = [&]<typename Table>(const std::shared_ptr<Table> & table)
    {
        if constexpr (is_join_map_forms<Table>)
        {
            if constexpr (requires { table->single->reserve(keys_to_reserve); })
                table->single->reserve(keys_to_reserve);
        }
        else if constexpr (requires { table->reserve(keys_to_reserve); })
            table->reserve(keys_to_reserve);
    };
    std::visit([&](auto & maps_) { maps_.visit(key_type, reserve_single); }, maps);
}

void HashJoinTable::setPartitionReserveOnFirstInsert(size_t keys_per_partition)
{
    partition_reserve_keys = keys_per_partition;
    auto set_reserve = [&]<typename Table>(const std::shared_ptr<Table> & table)
    {
        if constexpr (is_join_map_forms<Table>)
        {
            if (table->partitioned)
                table->partitioned->setReserveOnCreate(keys_per_partition);
        }
    };
    std::visit([&](auto & maps_) { maps_.visit(key_type, set_reserve); }, maps);
}

size_t HashJoinTable::arenaBytes() const
{
    size_t res = 0;
    for (const auto & arena : arenas)
        if (arena)
            res += arena->allocatedBytes();
    return res;
}

void HashJoinTable::noteNextArenaChunks()
{
    /// The growth for one allocation that does not fit the current chunk is the next chunk. An arena that holds nothing counts
    /// 0: its first chunk is at most a page, and a `GraceHashJoin` bucket, whose arenas start empty, could not split below the
    /// first chunks of all its arenas.
    for (size_t stream = 0; stream < arenas.size(); ++stream)
        if (const auto & arena = arenas[stream])
            next_arena_chunks[stream].store(
                arena->allocatedBytes() ? arena->estimateGrowthMemory(1, arena->remainingSpaceInCurrentMemoryChunk() + 1) : 0,
                std::memory_order_relaxed);
}

size_t HashJoinTable::nextArenaChunks(size_t streams) const
{
    size_t res = 0;
    for (size_t stream = 0; stream < std::min(streams, arenas.size()); ++stream)
        res += next_arena_chunks[stream].load(std::memory_order_relaxed);
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
    bool hashed = false;
    partitions_are_robin_hood = false;
    auto freeze_form = [&]<typename Form>(Form & form)
    {
        if constexpr (requires { form.freeze(); })
        {
            form.freeze();
            hashed = true;
            if constexpr (is_join_partitioned_view<Form>)
                partitions_are_robin_hood = form.isRobinHood();
        }
    };
    auto freeze_table = [&]<typename Table>(const std::shared_ptr<Table> & table)
    {
        if (!table)
            return;
        if constexpr (is_join_map_forms<Table>)
            table->visit(freeze_form);
        else
            freeze_form(*table);
    };
    std::visit([&](auto & maps_) { maps_.visit(key_type, freeze_table); }, maps);
    /// When the cells fit in cache, AMAC has few misses to overlap. On Graviton4, a single 2–32 MiB table
    /// was up to 18% slower with AMAC than with ordinary prefetch.
    static const size_t min_bytes_for_amac_lookup = getL2CacheSize() + getCPUDataCacheSize(3);
    if (hashed)
        amac_support = bufferBytes() > min_bytes_for_amac_lookup ? AmacSupport::LargeTable : AmacSupport::SmallTable;
    else
        amac_support = AmacSupport::Unsupported;
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
        {
            return maps_.visit(
                key_type, []<typename Table>(const std::shared_ptr<Table> &) { return sizeof(typename Table::cell_type); });
        },
        maps);
}

/// The keys are reinterpreted signed values, so the range is computed in wrapping unsigned arithmetic.
template <bool is_signed, typename Key, typename Maps>
NO_SANITIZE_UNSIGNED_OVERFLOW
bool HashJoinTable::tryConvertToFixedHashMapImpl(Maps & maps_)
{
    using SignedKey = std::make_signed_t<Key>;

    static constexpr size_t MAX_RANGE = (1ULL << 18);
    /// Limits conversion to cases where FixedHashMaps larger than 2^16 have at least 25% fill factor,
    /// ensuring they use at most around twice the memory of the source HashMap.
    static constexpr size_t MAX_RANGE_SPARSITY_FACTOR = 4;

    constexpr bool is_32 = std::is_same_v<Key, UInt32>;
    constexpr Type source_type = is_32 ? Type::key32 : Type::key64;
    auto & source_map = *maps_.template mapOf<source_type>()->single;

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
    auto convert_to_fixed_hash_map = [&]<size_t size_bits, Type range_type>() NO_SANITIZE_UNSIGNED_OVERFLOW
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
        /// The new map is direct-addressed.
        amac_support = AmacSupport::Unsupported;
        partitions_are_robin_hood = false;
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

/// A probe row reads its cell of the direct-addressed map, with no hash and no partition descriptor.
template <HashJoin::Type TYPE, typename Maps>
bool HashJoinTable::collapsePartitions(Maps & maps_)
{
    auto & table = *maps_.template mapOf<TYPE>();
    if (!table.partitioned)
        return false;

    using Table = std::decay_t<decltype(table)>;
    auto single = std::make_unique<typename Table::single_type>();
    auto & view = *table.partitioned;
    for (size_t p = 0; p < view.numPartitions(); ++p)
    {
        auto * map = view.partitionOrNull(p);
        if (!map)
            continue;
        for (auto it = map->begin(), end = map->end(); it != end; ++it)
        {
            typename Table::single_type::LookupResult res;
            bool inserted = false;
            single->emplace(it->getKey(), res, inserted);
            if constexpr (Maps::has_mapped)
                res->getMapped() = std::move(it->getMapped());
        }
    }
    table.single = std::move(single);
    table.partitioned.reset();
    num_partitions = 1;
    key_range = {0, 1ULL << (sizeof(typename Table::key_type) * 8)};
    amac_support = AmacSupport::Unsupported;
    partitions_are_robin_hood = false;
    return true;
}

bool HashJoinTable::tryConvertToFixedHashMap(bool is_signed)
{
    return std::visit(
        [&](auto & maps_)
        {
            using Maps = std::decay_t<decltype(maps_)>;
            if (key_type == Type::key8)
                return collapsePartitions<Type::key8>(maps_);
            if (key_type == Type::key16)
                return collapsePartitions<Type::key16>(maps_);
            if constexpr (std::is_same_v<Maps, HashJoin::MapsAsof>)
                return false;
            else if (isPartitioned())
                return false;
            else if (key_type == Type::key32)
                return is_signed ? tryConvertToFixedHashMapImpl<true, UInt32>(maps_) : tryConvertToFixedHashMapImpl<false, UInt32>(maps_);
            else
            {
                /// `HashJoin::canConvertToFixedHashMap` lets only `key32` and `key64` through.
                chassert(key_type == Type::key64);
                return is_signed ? tryConvertToFixedHashMapImpl<true, UInt64>(maps_) : tryConvertToFixedHashMapImpl<false, UInt64>(maps_);
            }
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

                auto probe_one = [&](size_t i) NO_SANITIZE_UNSIGNED_OVERFLOW -> UInt8
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
                        if (map.key8 && !map.key8->isPartitioned())
                            dispatch.template operator()<Int8, UInt8>(
                                std::shared_ptr<const typename std::decay_t<decltype(*map.key8)>::single_type>(
                                    map.key8, map.key8->single.get()),
                                0,
                                1ULL << 8);
                        break;
                    case Type::key16:
                        if (map.key16 && !map.key16->isPartitioned())
                            dispatch.template operator()<Int16, UInt16>(
                                std::shared_ptr<const typename std::decay_t<decltype(*map.key16)>::single_type>(map.key16, map.key16->single.get()),
                                0,
                                1ULL << 16);
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

/// Calls `f(view)` on the `ASOF` table of type `type` if it is a partitioned view. That leaves out the single form, which has
/// nothing to split.
template <typename F>
void visitAsofPartitionedView(const HashJoin::MapsVariant & maps, HashJoin::Type type, F && f)
{
    if (const auto * asof_maps = std::get_if<HashJoin::MapsAsof>(&maps))
        asof_maps->visit(type, [&]<typename Table>(const std::shared_ptr<Table> & table)
        {
            if constexpr (is_join_map_forms<Table>)
                if (table && table->partitioned)
                    f(*table->partitioned);
        });
}

}

HashJoinTable::ParallelClearWork HashJoinTable::parallelClearWork() const
{
    ParallelClearWork work;
    visitAsofPartitionedView(maps, key_type, [&](const auto & view)
    {
        work.partitions += view.numPartitions();
        work.keys += view.size();
    });
    return work;
}

void HashJoinTable::clearInParallel(size_t num_jobs, absl::FunctionRef<void(std::function<void()>)> schedule)
{
    visitAsofPartitionedView(maps, key_type, [&](auto & view)
    {
        for (size_t first = 0; first < num_jobs; ++first)
            schedule([&view, first, num_jobs]
            {
                for (size_t p = first; p < view.numPartitions(); p += num_jobs)
                    if (auto * map = view.partitionOrNull(p))
                        map->clearAndShrink();
            });
    });
}

void HashJoinTable::releaseMaps()
{
    std::visit([](auto & maps_) { maps_ = std::decay_t<decltype(maps_)>(); }, maps);
}

}
