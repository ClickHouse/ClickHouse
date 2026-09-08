#include <Interpreters/PartitionedHashJoin/JoinRouteHashing.h>

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/PartitionedHashJoin/DenseHyperLogLog.h>
#include <Interpreters/PartitionedHashJoin/SharedJoinTable.h>
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

template <typename KeyGetter, typename Hash>
void computeRoutesImpl(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, const UInt8 * skip, UInt16 * routes, DenseHyperLogLog & hll)
{
    /// The string getters hand out arena key holders; nothing persists them here, so the arena stays
    /// empty and only exists to satisfy the interface.
    Arena pool;
    KeyGetter key_getter(key_columns, key_sizes, nullptr);
    const Hash hash;
    for (size_t row = 0; row < rows; ++row)
    {
        auto && key_holder = key_getter.getKeyHolder(row, pool);
        const UInt64 mixed = sharedJoinMix(hash(keyHolderGetKey(key_holder)));
        routes[row] = static_cast<UInt16>(mixed >> 48);
        if (!skip || !skip[row])
            hll.add(static_cast<UInt32>(mixed >> 32));
    }
}

/// A direct-index table builds one partition and every key is its own cell, so the sketch only needs
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

template <HashJoin::Type type>
void computeRoutesForType(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, size_t rows, const UInt8 * skip, UInt16 * routes, DenseHyperLogLog & hll)
{
    using Table = typename std::remove_reference_t<decltype(SharedJoinTableDetail::MemberOf<type>::get(std::declval<SharedMapsAll &>()))>::element_type;
    using KeyGetter = typename KeyGetterForType<type, Table>::Type;
    if constexpr (is_shared_join_table<Table>)
        computeRoutesImpl<KeyGetter, typename Table::hash_type>(key_columns, key_sizes, rows, skip, routes, hll);
    else
        computeFixedRoutesImpl<KeyGetter>(key_columns, key_sizes, rows, skip, routes, hll);
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
    if (rows == 0)
        return;
    chassert(!key_columns.empty());

    switch (type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        computeRoutesForType<HashJoin::Type::TYPE>(key_columns, key_sizes, rows, skip, routes, hll); \
        return;
        APPLY_FOR_PARTITIONED_JOIN_VARIANTS(M)
#undef M
        default:
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys for the partitioned join (type: {})", type);
    }
}

}
