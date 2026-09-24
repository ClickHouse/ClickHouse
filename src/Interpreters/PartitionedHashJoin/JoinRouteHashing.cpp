#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/HashJoinTable.h>
#include <Common/Arena.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
extern const int UNSUPPORTED_JOIN_KEYS;
}

namespace
{

template <bool collect_sketch, typename KeyGetter, typename Hash>
void computeRoutesImpl(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, const UInt8 * skip, UInt16 * routes, DenseHyperLogLog * hll)
{
    /// The string getters hand out arena key holders. Nothing persists them here, so the arena stays
    /// empty and only exists to satisfy the interface.
    Arena pool;
    KeyGetter key_getter(key_columns, key_sizes, nullptr);
    const Hash hash;
    for (size_t row = 0; row < rows; ++row)
    {
        auto && key_holder = key_getter.getKeyHolder(row, pool);
        const size_t hash_value = hash(keyHolderGetKey(key_holder));
        routes[row] = static_cast<UInt16>(hashJoinTablePlacement(hash_value) >> 48);
        if constexpr (collect_sketch)
        {
            if (!skip || !skip[row])
                hll->add(static_cast<UInt32>(hashJoinTableMix(hash_value) >> 32));
        }
    }
}

/// A direct-index table builds one partition and every key is its own cell. The sketch only needs
/// to see distinct values; the key itself is a fine 32-bit word for it.
template <typename KeyGetter>
void computeFixedRoutesImpl(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, const UInt8 * skip, UInt16 * routes, DenseHyperLogLog & hll)
{
    std::fill_n(routes, rows, static_cast<UInt16>(0));
    Arena pool;
    KeyGetter key_getter(key_columns, key_sizes, nullptr);
    for (size_t row = 0; row < rows; ++row)
    {
        if (skip && skip[row])
            continue;
        auto && key_holder = key_getter.getKeyHolder(row, pool);
        hll.add(static_cast<UInt32>(keyHolderGetKey(key_holder)));
    }
}

template <bool collect_sketch, HashJoin::Type type, typename Table>
void computeRoutesForTable(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, const UInt8 * skip, UInt16 * routes, DenseHyperLogLog * hll)
{
    /// The routing only reads keys, so the getter needs no `JoinUsedFlags` offset.
    using KeyGetter = typename KeyGetterForType<type, Table, /*use_offset=*/false>::Type;
    if constexpr (is_hash_join_table<Table>)
        computeRoutesImpl<collect_sketch, KeyGetter, typename Table::hash_type>(key_columns, key_sizes, rows, skip, routes, hll);
    else if constexpr (collect_sketch)
        computeFixedRoutesImpl<KeyGetter>(key_columns, key_sizes, rows, skip, routes, *hll);
    else
        std::fill_n(routes, rows, static_cast<UInt16>(0));
}

template <bool collect_sketch>
void computeJoinRoutesForFillImpl(
    HashJoin::Type type,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt8 * skip,
    UInt16 * routes,
    DenseHyperLogLog * hll)
{
    if (rows == 0)
        return;
    chassert(!key_columns.empty());

    switch (type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        computeRoutesForTable<collect_sketch, HashJoin::Type::TYPE, typename decltype(HashJoinTableMapsAll::TYPE)::element_type>( \
            key_columns, key_sizes, rows, skip, routes, hll); \
        return;
        APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
        default:
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", type);
    }
}

}

void computeJoinRoutesForFill(
    HashJoin::Type type,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt8 * skip,
    UInt16 * routes,
    DenseHyperLogLog & hll)
{
    computeJoinRoutesForFillImpl<true>(type, key_columns, key_sizes, rows, skip, routes, &hll);
}

void computeJoinRoutesForFill(
    HashJoin::Type type,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    size_t rows,
    const UInt8 * skip,
    UInt16 * routes)
{
    computeJoinRoutesForFillImpl<false>(type, key_columns, key_sizes, rows, skip, routes, nullptr);
}

}
