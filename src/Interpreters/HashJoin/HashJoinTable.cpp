#include <Interpreters/HashJoin/HashJoinTable.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/joinDispatch.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

namespace DB
{

HashJoinTable::HashJoinTable(Type type, Sizes key_sizes_, JoinKind kind, JoinStrictness strictness, JoinMapsKind maps_kind, size_t num_slots)
    : key_sizes(std::move(key_sizes_))
    , slots(num_slots)
{
    joinDispatchInit(kind, strictness, maps, maps_kind);
    std::visit([&](auto & maps_) { maps_.create(type); }, maps);
}

void HashJoinTable::freezeForProbing(Type type)
{
    auto finish_fill = []<typename Table>(const std::shared_ptr<Table> & table)
    {
        if (!table)
            return;
        table->computeBucketPrefix();
        if constexpr (is_partitioned_fixed_table<Table>)
            table->restoreMinMaxOptimization();
    };
    std::visit([&](auto & maps_) { maps_.visit(type, finish_fill); }, maps);
}

size_t HashJoinTable::bufferBytes(Type type) const
{
    return std::visit([&](const auto & maps_) { return maps_.getTotalByteCountImpl(type); }, maps);
}

size_t HashJoinTable::bufferSizeInCells(Type type) const
{
    return std::visit([&](const auto & maps_) { return maps_.getBufferSizeInCells(type); }, maps);
}

template <bool is_signed, typename Key, typename Maps>
std::optional<HashJoinTable::FixedHashMapConversion> HashJoinTable::tryConvertToFixedHashMapImpl(Maps & maps_)
{
    using SignedKey = std::make_signed_t<Key>;

    static constexpr size_t MAX_RANGE = (1ULL << 18);
    /// Limits conversion to cases where FixedHashMaps larger than 2^16 have at least 25% fill factor,
    /// ensuring they use at most around twice the memory of the source HashMap.
    static constexpr size_t MAX_RANGE_SPARSITY_FACTOR = 4;

    auto & source_map = [&]() -> auto &
    {
        if constexpr (std::is_same_v<Key, UInt32>)
            return *maps_.key32;
        else
            return *maps_.key64;
    }();

    if (source_map.empty() || source_map.size() > MAX_RANGE)
        return {};

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
            return {};
    }

    size_t range = static_cast<size_t>(max_key - min_key) + 1;

    using Mapped = typename Maps::MappedType;
    std::optional<FixedHashMapConversion> conversion;
    auto convert_to_fixed_hash_map = [&]<size_t size_bits>(auto & dst_map, Type type)
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
        dst_map = std::move(range_map);
        conversion = FixedHashMapConversion{type, {min_key, range}, key_count};
    };

    auto dispatch_conversion =
        [&](auto & range8, Type type8, auto & range16, Type type16, auto & range17, Type type17, auto & range18, Type type18, auto & source)
    {
        if (range <= (1ULL << 8))
            convert_to_fixed_hash_map.template operator()<8>(range8, type8);
        else if (range <= (1ULL << 16))
            convert_to_fixed_hash_map.template operator()<16>(range16, type16);
        else if (range <= (1ULL << 17))
        {
            if ((1ULL << 17) > key_count * MAX_RANGE_SPARSITY_FACTOR)
                return;
            convert_to_fixed_hash_map.template operator()<17>(range17, type17);
        }
        else
        {
            if ((1ULL << 18) > key_count * MAX_RANGE_SPARSITY_FACTOR)
                return;
            convert_to_fixed_hash_map.template operator()<18>(range18, type18);
        }
        source.reset();
    };

    if constexpr (std::is_same_v<Key, UInt32>)
        dispatch_conversion(
            maps_.range8_key32,
            Type::range8_key32,
            maps_.range16_key32,
            Type::range16_key32,
            maps_.range17_key32,
            Type::range17_key32,
            maps_.range18_key32,
            Type::range18_key32,
            maps_.key32);
    else
        dispatch_conversion(
            maps_.range8_key64,
            Type::range8_key64,
            maps_.range16_key64,
            Type::range16_key64,
            maps_.range17_key64,
            Type::range17_key64,
            maps_.range18_key64,
            Type::range18_key64,
            maps_.key64);

    return conversion;
}

std::optional<HashJoinTable::FixedHashMapConversion> HashJoinTable::tryConvertToFixedHashMap(Type type, bool is_signed)
{
    return std::visit(
        [&](auto & maps_) -> std::optional<FixedHashMapConversion>
        {
            using Maps = std::decay_t<decltype(maps_)>;
            if constexpr (std::is_same_v<Maps, HashJoin::MapsAsof>)
                return {};
            else if (type == Type::key32)
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

/// Inner loop: for each probe value v of type T, check it falls within BuildKey's value range,
/// then narrow to BuildKey and reinterpret to the FixedHashMap's unsigned key. The null-mask
/// merge is split out of the loop body into two specialized paths so each loop stays branchless
/// and vectorizable.
template <typename BuildKey, typename HashMapT, typename T>
void probeFixedHashMapLoop(
    const HashMapT & ht,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size,
    const T * src,
    const UInt8 * null_map,
    UInt8 * result,
    size_t n)
{
    using UnsignedBK = std::make_unsigned_t<BuildKey>;
    static_assert(canLosslesslyHold<BuildKey, T>(),
                  "probeFixedHashMapLoop instantiated with a probe type that cannot hold BuildKey's full range");

    constexpr T t_lo = static_cast<T>(std::numeric_limits<BuildKey>::min());
    constexpr T t_hi = static_cast<T>(std::numeric_limits<BuildKey>::max());

    auto probe_one = [&](size_t i) -> UInt8
    {
        const T v = src[i];
        if (v < t_lo || v > t_hi)
            return 0;
        const UnsignedBK slot = static_cast<UnsignedBK>(static_cast<BuildKey>(v));
        const UnsignedBK idx = slot - min_key;
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
}

/// Dispatch over the probe column's element type to call probeFixedHashMapLoop.
/// Probe types whose value range can't hold BuildKey are filtered out at compile time;
/// at runtime a non-ColumnVector or excluded type is conservatively passed through (all 1).
template <typename BuildKey, typename HashMapT>
ColumnPtr probeFixedHashMap(
    const HashMapT & ht,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size,
    const ColumnWithTypeAndName & values)
{
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
                probeFixedHashMapLoop<BuildKey, HashMapT, T>(
                    ht, min_key, range_size,
                    typed_col.getData().data(), null_map, result, n);
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

/// Wrap a FixedHashMap as a ProbeFn for the runtime filter to invoke on the probe side.
template <typename BuildKey, typename HashMapT>
SharedFixedHashTableRuntimeFilter::ProbeFn buildSharedFilterProbeFn(
    std::shared_ptr<HashMapT> range_map_arg,
    std::make_unsigned_t<BuildKey> min_key,
    size_t range_size)
{
    return [range_map = std::move(range_map_arg), min_key, range_size]
        (const ColumnWithTypeAndName & values) -> ColumnPtr
    {
        return probeFixedHashMap<BuildKey, HashMapT>(*range_map, min_key, range_size, values);
    };
}

}

SharedFixedHashTableRuntimeFilter::ProbeFn HashJoinTable::makeSharedRuntimeFilter(Type type, KeyRange key_range, bool build_signed) const
{
    SharedFixedHashTableRuntimeFilter::ProbeFn probe_fn;
    std::visit(
        [&](const auto & map)
        {
            using MapType = std::decay_t<decltype(map)>;
            if constexpr (std::is_same_v<MapType, HashJoin::MapsOne> || std::is_same_v<MapType, HashJoin::MapsAll> || std::is_same_v<MapType, HashJoin::MapsSet>)
            {
                auto dispatch = [&]<typename BuildKey>(
                    const auto & range_ptr,
                    std::make_unsigned_t<BuildKey> min_key,
                    size_t range_size)
                {
                    if (!range_ptr)
                        return;
                    probe_fn = buildSharedFilterProbeFn<BuildKey>(range_ptr, min_key, range_size);
                };

                /// For range_*_key_* the min/range come from `tryConvertToFixedHashMap`'s
                /// computed key_range; for key8/key16 the whole key space is the table so
                /// min=0 and range = 2^(Key bits).
                switch (type)
                {
                    case Type::key8:
                        if (build_signed)
                            dispatch.template operator()<Int8>(map.key8, UInt8(0), 1ULL << 8);
                        else
                            dispatch.template operator()<UInt8>(map.key8, UInt8(0), 1ULL << 8);
                        break;
                    case Type::key16:
                        if (build_signed)
                            dispatch.template operator()<Int16>(map.key16, UInt16(0), 1ULL << 16);
                        else
                            dispatch.template operator()<UInt16>(map.key16, UInt16(0), 1ULL << 16);
                        break;
                    case Type::range8_key32:
                        if (build_signed)
                            dispatch.template operator()<Int32>(map.range8_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        else
                            dispatch.template operator()<UInt32>(map.range8_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        break;
                    case Type::range16_key32:
                        if (build_signed)
                            dispatch.template operator()<Int32>(map.range16_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        else
                            dispatch.template operator()<UInt32>(map.range16_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        break;
                    case Type::range17_key32:
                        if (build_signed)
                            dispatch.template operator()<Int32>(map.range17_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        else
                            dispatch.template operator()<UInt32>(map.range17_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        break;
                    case Type::range18_key32:
                        if (build_signed)
                            dispatch.template operator()<Int32>(map.range18_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        else
                            dispatch.template operator()<UInt32>(map.range18_key32,
                                static_cast<UInt32>(key_range.min_key), key_range.size);
                        break;
                    case Type::range8_key64:
                        if (build_signed)
                            dispatch.template operator()<Int64>(map.range8_key64,
                                key_range.min_key, key_range.size);
                        else
                            dispatch.template operator()<UInt64>(map.range8_key64,
                                key_range.min_key, key_range.size);
                        break;
                    case Type::range16_key64:
                        if (build_signed)
                            dispatch.template operator()<Int64>(map.range16_key64,
                                key_range.min_key, key_range.size);
                        else
                            dispatch.template operator()<UInt64>(map.range16_key64,
                                key_range.min_key, key_range.size);
                        break;
                    case Type::range17_key64:
                        if (build_signed)
                            dispatch.template operator()<Int64>(map.range17_key64,
                                key_range.min_key, key_range.size);
                        else
                            dispatch.template operator()<UInt64>(map.range17_key64,
                                key_range.min_key, key_range.size);
                        break;
                    case Type::range18_key64:
                        if (build_signed)
                            dispatch.template operator()<Int64>(map.range18_key64,
                                key_range.min_key, key_range.size);
                        else
                            dispatch.template operator()<UInt64>(map.range18_key64,
                                key_range.min_key, key_range.size);
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

/// Calls `f(map)` for each `ASOF` map of type `type` whose buckets have buffers of their own. A `PartitionedFixedHashTable`
/// has one buffer for all its buckets, and a map with one bucket has nothing to split.
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

HashJoinTable::ParallelClearWork HashJoinTable::parallelClearWork(Type type) const
{
    ParallelClearWork work;
    forEachAsofMapWithBucketBuffers(maps, type, [&]<typename Table>(const Table & map)
    {
        work.buckets += Table::NUM_BUCKETS;
        work.keys += map.size();
    });
    return work;
}

void HashJoinTable::clearInParallel(Type type, size_t num_jobs, absl::FunctionRef<void(std::function<void()>)> schedule)
{
    forEachAsofMapWithBucketBuffers(maps, type, [&]<typename Table>(Table & table)
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
