#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/HashJoin/JoinFeatures.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/KnownRowsHolder.h>
#include <Interpreters/HashJoin/JoinUsedFlags.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <typeinfo>

namespace DB
{

/// Prefetching doesn't make sense for small hash tables, because they fit in caches entirely.
/// Returns the threshold (in bytes) above which prefetching is enabled in JOIN.
size_t getMinBytesForPrefetchInJoin();

/// Inserting an element into a hash table of the form `key -> reference to a row`, which will then be used by JOIN.
template <typename HashMap, typename KeyGetter>
struct Inserter
{
    /// `new_keys` counts keys the map lacked, not rows accepted: `any_take_last_row` overwrites.
    static ALWAYS_INLINE bool insertOne(
        const HashJoin & join,
        HashMap & map,
        KeyGetter & key_getter,
        UInt32 stored_block_no,
        size_t key_row,
        size_t row_no,
        Arena & pool,
        size_t & new_keys)
    {
        auto emplace_result = key_getter.emplaceKey(map, key_row, pool);

        const bool inserted = emplace_result.isInserted();
        new_keys += inserted;
        if (inserted || join.anyTakeLastRow())
            new (&emplace_result.getMapped()) HashMap::mapped_type(stored_block_no, row_no);
        return inserted || join.anyTakeLastRow();
    }

    static ALWAYS_INLINE bool insertAll(
        const HashJoin &,
        HashMap & map,
        KeyGetter & key_getter,
        UInt32 stored_block_no,
        size_t key_row,
        size_t row_no,
        Arena & pool,
        size_t & new_keys)
    {
        auto emplace_result = key_getter.emplaceKey(map, key_row, pool);

        const bool inserted = emplace_result.isInserted();
        new_keys += inserted;
        if (inserted)
            new (&emplace_result.getMapped()) HashMap::mapped_type(stored_block_no, row_no);
        else
        {
            /// A single ref is stored inline in the value of the hash table; the first duplicate
            /// switches the value to a pointer to an arena-allocated list of refs.
            emplace_result.getMapped().insert(RowRef(stored_block_no, row_no).encode(), pool);
        }
        return inserted;
    }

    static ALWAYS_INLINE bool insertAsof(
        HashJoin & join,
        HashMap & map,
        KeyGetter & key_getter,
        UInt32 stored_block_no,
        size_t key_row,
        size_t row_no,
        Arena & pool,
        size_t & new_keys,
        const IColumn & asof_column)
    {
        auto emplace_result = key_getter.emplaceKey(map, key_row, pool);
        auto * time_series_map = &emplace_result.getMapped();

        const bool inserted = emplace_result.isInserted();
        new_keys += inserted;
        TypeIndex asof_type = *join.getAsofType();
        if (inserted)
            time_series_map = new (time_series_map) HashMap::mapped_type(createAsofRowRef(asof_type, join.getAsofInequality()));
        (*time_series_map)->insert(asof_column, stored_block_no, row_no);
        return inserted;
    }
};

/// The one key getter shared by a block's slots, for the getters whose construction reads the whole
/// block. Type-erased because the concrete type is only known inside the per-key-type dispatch.
class BlockKeyGetter
{
public:
    template <typename KeyGetter, typename Build>
    KeyGetter & getOrBuild(Build && build)
    {
        if (!getter)
        {
            getter = std::make_shared<KeyGetter>(build());
            built_type = &typeid(KeyGetter);
        }
        chassert(*built_type == typeid(KeyGetter));
        return *static_cast<KeyGetter *>(getter.get());
    }

private:
    std::shared_ptr<void> getter;
    const std::type_info * built_type = nullptr;
};

template <typename KeyGetter>
constexpr bool shareKeyGetterAcrossBuckets()
{
    if constexpr (requires { KeyGetter::reads_whole_block_at_construction; })
        return KeyGetter::reads_whole_block_at_construction;
    else
        return false;
}

/// MapsTemplate is one of MapsOne, MapsAll and MapsAsof
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate> // NOLINT(readability-identifier-naming)
class HashJoinMethods
{
    static constexpr bool needs_offset = JoinFeatures<KIND, STRICTNESS, MapsTemplate>::need_flags;

public:
    static void insertFromBlockImpl(
        HashJoin & join,
        HashJoin::Type type,
        MapsTemplate & maps,
        BlockKeyGetter & block_key_getter,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        UInt32 stored_block_no,
        const ScatteredBlock::Selector & selector,
        const Columns * dense_keys,
        ConstNullMapPtr null_map,
        const JoinCommon::JoinMask & join_mask,
        Arena & pool,
        BuildResult & result);

    using MapsTemplateVector = std::vector<const MapsTemplate *>;

    static JoinResultPtr joinBlockImpl(
        const HashJoin & join,
        Block block,
        const Block & block_with_columns_to_add,
        const MapsTemplateVector & maps_,
        bool is_join_get = false);

    static JoinResultPtr joinBlockImpl(
        const HashJoin & join,
        ScatteredBlock block,
        const Block & block_with_columns_to_add,
        const MapsTemplateVector & maps_,
        bool is_join_get = false);

private:
    template <typename KeyGetter, bool is_asof_join>
    static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes, HashJoin::RightTableData::KeyRange key_range = {});

    template <typename KeyGetter, bool is_asof_join>
    static KeyGetter & blockKeyGetter(
        BlockKeyGetter & block_key_getter, std::optional<KeyGetter> & own, const ColumnRawPtrs & key_columns, const Sizes & key_sizes);

    template <typename KeyGetter, typename HashMap, typename Selector>
    static void insertFromBlockImplTypeCase(
        HashJoin & join,
        HashMap & map,
        BlockKeyGetter & block_key_getter,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        UInt32 stored_block_no,
        const Selector & selector,
        const Columns * dense_keys,
        ConstNullMapPtr null_map,
        const JoinCommon::JoinMask & join_mask,
        Arena & pool,
        BuildResult & result);

    template <typename AddedColumns>
    static size_t switchJoinRightColumns(
        const std::vector<const MapsTemplate *> & mapv,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        HashJoin::Type type,
        JoinStuff::JoinUsedFlags & used_flags,
        HashJoin::RightTableData::KeyRange key_range);

    template <typename KeyGetter, typename Map, typename AddedColumns>
    static size_t joinRightColumnsSwitchNullability(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        JoinStuff::JoinUsedFlags & used_flags);

    template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns>
    static size_t joinRightColumnsSwitchMultipleDisjuncts(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        JoinStuff::JoinUsedFlags & used_flags);

    /// Joins right table columns which indexes are present in right_indexes using specified map.
    /// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
    template <
        typename KeyGetter,
        typename Map,
        bool need_filter,
        typename AddedColumns,
        typename Selector>
    static size_t joinRightColumns(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags,
        const Selector & selector);

    template <
        typename KeyGetter,
        typename Map,
        bool need_filter,
        typename AddedColumns,
        typename Selector>
    static size_t joinRightColumns(
        KeyGetter & key_getter,
        const Map * map,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags,
        const Selector & selector);

    /// First to collect all matched rows refs by join keys, then filter out rows which are not true in additional filter expression.
    template <typename KeyGetter, typename Map, typename AddedColumns>
    static size_t joinRightColumnsWithAdditionalFilter(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
        const ScatteredBlock::Selector & selector,
        bool need_filter [[maybe_unused]],
        bool flag_per_row [[maybe_unused]]);
};

/// Instantiate template class ahead in different .cpp files to avoid `too large translation unit`.
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::RightAny, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Asof, HashJoin::MapsAsof>;

extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::RightAny, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Semi, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Anti, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Right, JoinStrictness::Asof, HashJoin::MapsAsof>;

extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::RightAny, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::RightAny, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Semi, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Anti, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Inner, JoinStrictness::Asof, HashJoin::MapsAsof>;

extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::RightAny, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Any, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::All, HashJoin::MapsAll>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Semi, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Anti, HashJoin::MapsOne>;
extern template class HashJoinMethods<JoinKind::Full, JoinStrictness::Asof, HashJoin::MapsAsof>;
}
