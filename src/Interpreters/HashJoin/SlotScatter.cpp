#include <Interpreters/HashJoin/SlotScatter.h>

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>

#include <base/defines.h>

#include <utility>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

/// Routes each row the way `map` places its key: by `map.hash` and `getBucketOfKey`.
template <HashJoin::Type type, typename Map>
SlotScatter scatterImpl(
    const Map & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots,
    bool is_asof)
{
    using KeyGetter = KeyGetterForType<type, Map, false>::Type;

    if constexpr (requires { KeyGetter::has_pre_computed_hashes; })
        static_assert(!KeyGetter::has_pre_computed_hashes, "Bucket routing assumes the map computes the hash it places by");

    /// For ASOF the key getter leaves out the ASOF column; `dense_keys` below still gathers it with the other keys.
    KeyGetter key_getter
        = is_asof ? createKeyGetter<KeyGetter, true>(key_columns, key_sizes) : createKeyGetter<KeyGetter, false>(key_columns, key_sizes);

    /// Nothing here outlives the call: the key holders are read for their hash, never persisted.
    Arena scratch_pool;

    const size_t rows = selector.size();

    PODArray<UInt32> row_to_slot(rows);
    std::vector<size_t> counts(num_slots, 0);
    for (size_t i = 0; i < rows; ++i)
    {
        auto key_holder = key_getter.getKeyHolder(selector[i], scratch_pool);
        const auto & key = keyHolderGetKey(key_holder);

        const size_t bucket = getBucketOfKey<Map>(key, map.hash(key));
        const auto slot = static_cast<UInt32>(BuildSlots::slotForBucket(bucket, num_slots));
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

template <typename Maps>
SlotScatter scatterBlockBySlot(
    HashJoin::Type type,
    const Maps & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots)
{
    /// `MapsAsof` is the map of every ASOF clause and of no other strictness.
    constexpr bool is_asof = std::is_same_v<Maps, HashJoin::MapsAsof>;
    switch (type)
    {
#define M(NAME) \
        case HashJoin::Type::NAME: \
            return scatterImpl<HashJoin::Type::NAME>(*maps.NAME, key_columns, key_sizes, selector, num_slots, is_asof);
            APPLY_FOR_TWO_LEVEL_JOIN_VARIANTS(M)
#undef M
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Hash join map type {} is not split into slots", type);
    }
}

template SlotScatter scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsOne &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);
template SlotScatter scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsAll &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);
template SlotScatter scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsAsof &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);
template SlotScatter scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsSet &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);

}
