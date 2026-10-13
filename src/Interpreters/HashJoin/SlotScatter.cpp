#include <Interpreters/HashJoin/SlotScatter.h>

#include <Interpreters/HashJoin/KeyGetter.h>
#include <Common/Arena.h>
#include <Common/PODArray.h>

#include <base/defines.h>

#include <utility>
#include <vector>

namespace DB
{

namespace
{

/// Routes each row the way `map` places its key: by `map.hash` and `getBucketOfKey`.
template <HashJoin::Type type, typename Map>
std::vector<ScatteredBlock::Selector> scatterImpl(
    const Map & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ScatteredBlock::Selector & selector,
    size_t num_slots,
    bool is_asof)
{
    using KeyGetter = KeyGetterForType<type, Map>::Type;

    if constexpr (requires { KeyGetter::has_pre_computed_hashes; })
        static_assert(!KeyGetter::has_pre_computed_hashes, "Bucket routing assumes the map computes the hash it places by");

    /// For ASOF the key getter leaves out the ASOF column.
    KeyGetter key_getter = is_asof
        ? KeyGetter(ColumnRawPtrs(key_columns.begin(), key_columns.end() - 1), Sizes(key_sizes.begin(), key_sizes.end() - 1), nullptr)
        : KeyGetter(key_columns, key_sizes, nullptr);

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
        /// The slot count is a power of two, so this is `bucket % num_slots`.
        const auto slot = static_cast<UInt32>(bucket & (num_slots - 1));
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

    std::vector<ScatteredBlock::Selector> result;
    result.reserve(num_slots);
    for (auto & column : indexes)
        result.emplace_back(std::move(column));

    return result;
}

}

template <typename Maps>
std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
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
            UNREACHABLE();
    }
}

template std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsOne &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);
template std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsAll &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);
template std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsAsof &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);
template std::vector<ScatteredBlock::Selector> scatterBlockBySlot(
    HashJoin::Type, const HashJoin::MapsSet &, const ColumnRawPtrs &, const Sizes &, const ScatteredBlock::Selector &, size_t);

}
