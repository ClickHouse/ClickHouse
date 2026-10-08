#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/HashJoin/JoinFeatures.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/KnownRowsHolder.h>
#include <Interpreters//HashJoin/JoinUsedFlags.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>

namespace DB
{
/// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
template <typename HashMap, typename KeyGetter>
struct Inserter
{
    static ALWAYS_INLINE bool
    insertOne(const HashJoin & join, HashMap & map, KeyGetter & key_getter, const ColumnsInfo * stored_columns_info, size_t i, Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if (emplace_result.isInserted() || join.anyTakeLastRow())
            new (&emplace_result.getMapped()) typename HashMap::mapped_type(stored_columns_info, i);
        return emplace_result.isInserted() || join.anyTakeLastRow();
    }

    static ALWAYS_INLINE bool
    insertAll(const HashJoin &, HashMap & map, KeyGetter & key_getter, const ColumnsInfo * stored_columns_info, size_t i, Arena & pool)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);

        if (emplace_result.isInserted())
            new (&emplace_result.getMapped()) typename HashMap::mapped_type(stored_columns_info, i);
        else
        {
            /// The first element of the list is stored in the value of the hash table, the rest in the pool.
            emplace_result.getMapped().insert({stored_columns_info, i}, pool);
        }
        return emplace_result.isInserted();
    }

    static ALWAYS_INLINE bool insertAsof(
        HashJoin & join,
        HashMap & map,
        KeyGetter & key_getter,
        const ColumnsInfo * stored_columns_info,
        size_t i,
        Arena & pool,
        const IColumn & asof_column)
    {
        auto emplace_result = key_getter.emplaceKey(map, i, pool);
        typename HashMap::mapped_type * time_series_map = &emplace_result.getMapped();

        TypeIndex asof_type = *join.getAsofType();
        if (emplace_result.isInserted())
            time_series_map = new (time_series_map) typename HashMap::mapped_type(createAsofRowRef(asof_type, join.getAsofInequality()));
        (*time_series_map)->insert(asof_column, stored_columns_info, i);
        return emplace_result.isInserted();
    }
};

/// MapsTemplate is one of MapsOne, MapsAll and MapsAsof
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
class HashJoinMethods
{
public:
    static void insertFromBlockImpl(
        HashJoin & join,
        HashJoin::Type type,
        MapsTemplate & maps,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        const ColumnsInfo * stored_columns_info,
        const ScatteredBlock::Selector & selector,
        ConstNullMapPtr null_map,
        const JoinCommon::JoinMask & join_mask,
        Arena & pool,
        bool & is_inserted,
        bool & all_values_unique);

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
    static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes);

    template <typename KeyGetter, typename HashMap, typename Selector>
    static void insertFromBlockImplTypeCase(
        HashJoin & join,
        HashMap & map,
        const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes,
        const ColumnsInfo * stored_columns_info,
        const Selector & selector,
        ConstNullMapPtr null_map,
        const JoinCommon::JoinMask & join_mask,
        Arena & pool,
        bool & is_inserted,
        bool & all_values_unique);

    template <typename AddedColumns>
    static size_t switchJoinRightColumns(
        const std::vector<const MapsTemplate *> & mapv,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        HashJoin::Type type,
        JoinStuff::JoinUsedFlags & used_flags);

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
        bool check_null_map,
        JoinCommon::JoinMask::Kind join_mask_kind,
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
        bool check_null_map,
        typename AddedColumns,
        typename Selector>
    static size_t joinRightColumnsSwitchJoinMaskKind(
        std::vector<KeyGetter> && key_getter_vector,
        const std::vector<const Map *> & mapv,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags,
        const Selector & selector);

    template <
        typename KeyGetter,
        typename Map,
        bool need_filter,
        bool check_null_map,
        JoinCommon::JoinMask::Kind join_mask_kind,
        typename AddedColumns,
        typename Selector>
    static size_t joinRightColumns(
        KeyGetter & key_getter,
        const Map * map,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags,
        const Selector & selector);

    template <typename KeyGetter, typename Map, bool need_filter, bool check_null_map, typename AddedColumns, typename Selector>
    static size_t joinRightColumnsSwitchJoinMaskKind(
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

    /// Cut first num_rows rows from block in place and returns block with remaining rows
    static Block sliceBlock(Block & block, size_t num_rows);

    /** Since we do not store right key columns,
      * this function is used to copy left key columns to right key columns.
      * If the user requests some right columns, we just copy left key columns to right, since they are equal.
      * Example: SELECT t1.key, t2.key FROM t1 FULL JOIN t2 ON t1.key = t2.key;
      * In that case for matched rows in t2.key we will use values from t1.key.
      * However, in some cases we might need to adjust the type of column, e.g. t1.key :: LowCardinality(String) and t2.key :: String
      * Also, the nullability of the column might be different.
      * Returns the right column after with necessary adjustments.
      */
    static ColumnWithTypeAndName copyLeftKeyColumnToRight(
        const DataTypePtr & right_key_type,
        const String & renamed_right_column,
        const ColumnWithTypeAndName & left_column,
        const IColumn::Filter * null_map_filter = nullptr);

    static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable);

    static void correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable, const IColumn::Filter & negative_null_map);
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
