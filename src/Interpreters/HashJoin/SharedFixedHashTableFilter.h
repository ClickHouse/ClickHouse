#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/HashJoin/HashJoinTypes.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>

#include <memory>
#include <type_traits>
#include <utility>

namespace DB
{

/** The exact runtime filter a built fixed hash table gives the probe side. The planner's
  * `BuildRuntimeFilterStep` installs a Bloom or Set filter over the build keys; when the build side ended
  * up in a table indexed by key value - the 8- and 16-bit key maps, or a `range*` map the post-build
  * conversion built from a dense `key32` / `key64` table - a membership probe of that table is exact and
  * cheaper, so it replaces the planner's filter under the same lookup key. The maps struct is a template
  * parameter: `HashJoinTypes::MapsTemplate` and the partitioned join's shared maps name their fixed tables
  * alike, and the `shared_ptr` the probe function captures keeps the table alive past the join.
  */
namespace SharedFixedHashTableFilterDetail
{

/// Whether every value of `BuildKey` fits `T`, the probe column's type after the join's common-type cast.
/// `getLeastSupertype` guarantees it; a violation passes every row through instead of misfiring the range check.
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

/// `BuildKey`'s value range expressed in `T`, which `canLosslesslyHold` guarantees is wide enough. Derived
/// from the key's width rather than from `std::numeric_limits<BuildKey>`: widening an `Int8` limit reads
/// as a character misuse to `bugprone-signed-char-misuse`.
template <typename BuildKey, typename T>
constexpr std::pair<T, T> valueRangeOf()
{
    if constexpr (sizeof(T) == sizeof(BuildKey))
        return {std::numeric_limits<T>::min(), std::numeric_limits<T>::max()};
    else if constexpr (std::is_signed_v<BuildKey>)
    {
        constexpr Int64 half = Int64(1) << (sizeof(BuildKey) * 8 - 1);
        return {static_cast<T>(-half), static_cast<T>(half - 1)};
    }
    else
        return {T(0), static_cast<T>((UInt64(1) << (sizeof(BuildKey) * 8)) - 1)};
}

/// One probe value is a member when it lies in `BuildKey`'s value range, and shifted by `min_key` lies in
/// the table's range and the table has it. The null-mask merge is split into two loops so that each stays
/// branchless and vectorizable.
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
    static_assert(canLosslesslyHold<BuildKey, T>(), "probeFixedHashMapLoop instantiated with a probe type that cannot hold BuildKey's full range");

    constexpr std::pair<T, T> value_range = valueRangeOf<BuildKey, T>();
    constexpr T t_lo = value_range.first;
    constexpr T t_hi = value_range.second;

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

/// Dispatches on the probe column's element type. A column that is not a `ColumnVector` of an integer
/// type able to hold `BuildKey` passes every row (all 1).
template <typename BuildKey, typename HashMapT>
ColumnPtr probeFixedHashMap(const HashMapT & ht, std::make_unsigned_t<BuildKey> min_key, size_t range_size, const ColumnWithTypeAndName & values)
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
        ColumnVector<UInt8>,
        ColumnVector<UInt16>,
        ColumnVector<UInt32>,
        ColumnVector<UInt64>,
        ColumnVector<Int8>,
        ColumnVector<Int16>,
        ColumnVector<Int32>,
        ColumnVector<Int64>>(
        col,
        [&](const auto & typed_col) -> bool
        {
            using T = typename std::decay_t<decltype(typed_col)>::ValueType;
            if constexpr (canLosslesslyHold<BuildKey, T>())
            {
                probeFixedHashMapLoop<BuildKey, HashMapT, T>(ht, min_key, range_size, typed_col.getData().data(), null_map, result, n);
                return true;
            }
            else
            {
                return false;
            }
        });

    if (!dispatched)
        std::fill_n(result, n, static_cast<UInt8>(1));

    return result_col;
}

/// Wraps one fixed hash table as the probe function of `SharedFixedHashTableRuntimeFilter`.
template <typename BuildKey, typename HashMapT>
SharedFixedHashTableRuntimeFilter::ProbeFn
buildSharedFilterProbeFn(std::shared_ptr<HashMapT> range_map_arg, std::make_unsigned_t<BuildKey> min_key, size_t range_size)
{
    return [range_map = std::move(range_map_arg), min_key, range_size](const ColumnWithTypeAndName & values) -> ColumnPtr
    {
        return probeFixedHashMap<BuildKey, HashMapT>(*range_map, min_key, range_size, values);
    };
}

}

/// The types whose table is indexed by key value: the 8- and 16-bit key maps over their whole key space,
/// and the range maps over `[min_key, min_key + size)`.
inline bool isFixedHashTableType(HashJoinTypes::Type type)
{
    using enum HashJoinTypes::Type;
    switch (type)
    {
        case key8:
        case key16:
        case range8_key32:
        case range16_key32:
        case range17_key32:
        case range18_key32:
        case range8_key64:
        case range16_key64:
        case range17_key64:
        case range18_key64:
            return true;
        default:
            return false;
    }
}

/// The membership probe over the fixed table `type` names in `maps`, for a build column that is the
/// signed or unsigned counterpart of the table's key. Empty when `type` is not a fixed table or the
/// table was never created.
template <typename Maps>
SharedFixedHashTableRuntimeFilter::ProbeFn sharedFixedHashTableProbeFn(
    const Maps & maps, HashJoinTypes::Type type, HashJoinTypes::RightTableData::KeyRange key_range, bool build_signed)
{
    using ProbeFn = SharedFixedHashTableRuntimeFilter::ProbeFn;
    auto over = [&]<typename Unsigned>(const auto & table, Unsigned min_key, size_t range_size) -> ProbeFn
    {
        if (!table)
            return {};
        if (build_signed)
            return SharedFixedHashTableFilterDetail::buildSharedFilterProbeFn<std::make_signed_t<Unsigned>>(table, min_key, range_size);
        return SharedFixedHashTableFilterDetail::buildSharedFilterProbeFn<Unsigned>(table, min_key, range_size);
    };

    /// The 8- and 16-bit maps span their whole key space; a range map's bounds come from the conversion.
    switch (type)
    {
        case HashJoinTypes::Type::key8:
            return over(maps.key8, UInt8(0), 1uz << 8);
        case HashJoinTypes::Type::key16:
            return over(maps.key16, UInt16(0), 1uz << 16);
#define M(NAME, KEY) \
    case HashJoinTypes::Type::NAME: \
        return over(maps.NAME, static_cast<KEY>(key_range.min_key), key_range.size);
            M(range8_key32, UInt32)
            M(range16_key32, UInt32)
            M(range17_key32, UInt32)
            M(range18_key32, UInt32)
            M(range8_key64, UInt64)
            M(range16_key64, UInt64)
            M(range17_key64, UInt64)
            M(range18_key64, UInt64)
#undef M
        default:
            return {};
    }
}

/// Replaces the query's runtime filters registered on `build_key_name` with `probe_fn`, under the keys
/// the planner recorded in `table_join`'s shared filter descriptors. Nothing happens outside a query.
void replaceSharedRuntimeFilters(
    const TableJoin & table_join, const String & build_key_name, const SharedFixedHashTableRuntimeFilter::ProbeFn & probe_fn);

/// Publishes the exact filter of a built join whose table is a fixed hash table (`type`), when the setting
/// asks for it and the planner recorded descriptors. It needs one integer build key and more than one
/// distinct key: for one key the `== const` specialization of the runtime filter is faster.
template <typename Maps>
void publishSharedFixedHashTableFilters(
    const TableJoin & table_join,
    const Block & right_table_keys,
    HashJoinTypes::Type type,
    HashJoinTypes::RightTableData::KeyRange key_range,
    size_t keys_to_join,
    const Maps & maps)
{
    if (!table_join.joinRuntimeFilterFromFixedHashTable() || table_join.getSharedRuntimeFilterDescriptors().empty())
        return;
    if (!isFixedHashTableType(type) || keys_to_join <= 1 || right_table_keys.columns() != 1)
        return;

    /// Only an integer-represented build type has the min / max the range check needs: `Float`, `Decimal`
    /// and `DateTime64` (a scale) drop out here.
    const auto build_type = removeNullable(right_table_keys.getByPosition(0).type);
    if (!build_type->isValueRepresentedByInteger())
        return;
    const bool build_signed = !build_type->isValueRepresentedByUnsignedInteger();

    auto probe_fn = sharedFixedHashTableProbeFn(maps, type, key_range, build_signed);
    if (!probe_fn)
        return;
    replaceSharedRuntimeFilters(table_join, right_table_keys.getByPosition(0).name, probe_fn);
}

}
