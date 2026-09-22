#include <Interpreters/HashJoin/SlotScatter.h>

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>

#include <base/defines.h>

#include <utility>
#include <vector>

namespace DB
{
namespace
{

/// `routingHashForRow` wants a map to hash with, and there is no instance here.
template <typename RepMap>
struct ScatterHashAdapter
{
    template <typename K>
    size_t hash(const K & key) const { return RepMap::hash(key); }
};

/// Instantiated per key type instead of per (kind, strictness, mapped type). `RepMap` stands in for
/// the clause's map - any mapped type will do except for a fixed-range one, see `MapsKind`. Routing
/// forwards to the map's own statics, so the two cannot disagree.
template <HashJoin::Type type, typename RepMap>
SlotScatter scatterImpl(
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots,
    bool is_asof)
{
    using KeyGetter = KeyGetterForType<type, RepMap, false>::Type;

    if constexpr (requires { KeyGetter::has_pre_computed_hashes; })
        static_assert(!KeyGetter::has_pre_computed_hashes, "Bucket routing assumes the map computes the hash it places by");

    /// The range maps learn their real range only after the build, so the default (unshifted) one is
    /// right here. `dense_keys` below still gathers the full key list, ASOF column included.
    KeyGetter key_getter
        = is_asof ? createKeyGetter<KeyGetter, true>(key_columns, key_sizes) : createKeyGetter<KeyGetter, false>(key_columns, key_sizes);

    static constexpr ScatterHashAdapter<RepMap> hash_adapter{};

    /// Nothing here outlives the call: the key holders are read for their hash, never persisted.
    Arena scratch_pool;

    const size_t rows = selector.size();

    PODArray<UInt32> row_to_slot(rows);
    std::vector<size_t> counts(num_slots, 0);
    for (size_t i = 0; i < rows; ++i)
    {
        auto key_holder = key_getter.getKeyHolder(selector[i], scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);

        size_t hash_value = 0;
        if constexpr (requires { key_getter.routingHashForRow(hash_adapter, selector[i], scratch_pool); })
            hash_value = key_getter.routingHashForRow(hash_adapter, selector[i], scratch_pool);
        else
            hash_value = RepMap::hash(key);

        const size_t bucket = RepMap::getBucketFromHash(RepMap::bucketRoutingHash(key, hash_value));
        const auto slot = static_cast<UInt32>(slotForBucket(bucket, num_slots));
        row_to_slot[i] = slot;
        ++counts[slot];
    }

    std::vector<ScatteredBlock::Selector::IndexesPtr> indexes;
    indexes.reserve(num_slots);
    for (size_t slot = 0; slot < num_slots; ++slot)
    {
        auto column = ScatteredBlock::Selector::Indexes::create();
        column->getData().reserve(counts[slot]);
        indexes.push_back(std::move(column));
    }

    for (size_t i = 0; i < rows; ++i)
        indexes[row_to_slot[i]]->getData().push_back(selector[i]);

    SlotScatter result;
    result.selectors.reserve(num_slots);
    for (auto & column : indexes)
        result.selectors.emplace_back(std::move(column));

    /// Gathering the keys only pays for itself when a row's keys are no wider than the selector index
    /// the insert loop would otherwise read; `+ 1` is how a column of unbounded width says "over budget".
    constexpr size_t selector_bytes_per_row = sizeof(IColumn::Selector::value_type);
    size_t max_bytes_per_row = 0;
    for (const auto * column : key_columns)
        max_bytes_per_row
            += (column->valuesHaveFixedSize() && !column->lowCardinality()) ? column->sizeOfValueIfFixed() : selector_bytes_per_row + 1;

    const bool selector_is_identity = selector.isContinuousRange() && selector.getRange().first == 0
        && !key_columns.empty() && selector.getRange().second == key_columns[0]->size();

    if (max_bytes_per_row <= selector_bytes_per_row && selector_is_identity)
    {
        IColumn::Selector column_selector(rows);
        for (size_t i = 0; i < rows; ++i)
            column_selector[i] = row_to_slot[i];

        result.dense_keys.resize(num_slots);
        for (const auto * column : key_columns)
        {
            auto parts = column->scatter(num_slots, column_selector);
            chassert(parts.size() == num_slots);
            for (size_t slot = 0; slot < num_slots; ++slot)
                result.dense_keys[slot].push_back(std::move(parts[slot]));
        }
    }

    return result;
}

}

SlotScatter scatterBlockBySlot(
    HashJoin::Type type,
    MapsKind maps_kind,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots)
{
    /// `MapsAsof` is the map of every ASOF clause and of no other strictness.
    const bool is_asof = maps_kind == MapsKind::Asof;
    switch (type)
    {
#define M(NAME) \
        case HashJoin::Type::NAME: \
        { \
            using MapOne = typename decltype(std::declval<HashJoin::MapsOne>().NAME)::element_type; \
            if constexpr (MapOne::isFixedRangeStorage()) \
            { \
                switch (maps_kind) \
                { \
                    case MapsKind::One: \
                        return scatterImpl<HashJoin::Type::NAME, MapOne>( \
                            key_columns, key_sizes, selector, num_slots, is_asof); \
                    case MapsKind::All: \
                    { \
                        using MapAll = typename decltype(std::declval<HashJoin::MapsAll>().NAME)::element_type; \
                        return scatterImpl<HashJoin::Type::NAME, MapAll>( \
                            key_columns, key_sizes, selector, num_slots, is_asof); \
                    } \
                    case MapsKind::Asof: \
                    { \
                        using MapAsof = typename decltype(std::declval<HashJoin::MapsAsof>().NAME)::element_type; \
                        return scatterImpl<HashJoin::Type::NAME, MapAsof>( \
                            key_columns, key_sizes, selector, num_slots, is_asof); \
                    } \
                    case MapsKind::Set: \
                    { \
                        using MapSet = typename decltype(std::declval<HashJoin::MapsSet>().NAME)::element_type; \
                        return scatterImpl<HashJoin::Type::NAME, MapSet>( \
                            key_columns, key_sizes, selector, num_slots, is_asof); \
                    } \
                } \
            } \
            else \
            { \
                return scatterImpl<HashJoin::Type::NAME, MapOne>( \
                    key_columns, key_sizes, selector, num_slots, is_asof); \
            } \
        }

            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
}

}
