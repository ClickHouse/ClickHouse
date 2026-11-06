#pragma once
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/HashJoin/HashJoinTable.h>
#include <Interpreters/HashJoin/KeyGetter.h>
#include <Interpreters/HashJoin/JoinFeatures.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/KnownRowsHolder.h>
#include <Interpreters//HashJoin/JoinUsedFlags.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/castColumn.h>
#include <base/types.h>
#include <span>

namespace DB
{

/// MapsTemplate is one of MapsOne, MapsAll, MapsAsof and MapsSet
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
class HashJoinMethods
{
public:
    /// Inserts the rows of a stored block into `table`, allocating in the arena of build stream `stream`.
    static BuildResult insertFromBlockImpl(HashJoin & join, HashJoinTable & table, const HashJoinTable::InsertRows & rows, size_t stream);

    static JoinResultPtr joinBlockImpl(
        const HashJoin & join,
        Block block,
        const Block & block_with_columns_to_add,
        bool is_join_get = false);

    static JoinResultPtr joinBlockImpl(
        const HashJoin & join,
        ScatteredBlock block,
        const Block & block_with_columns_to_add,
        bool is_join_get = false);

private:
    using Tables = std::span<const std::unique_ptr<HashJoinTable>>;

    template <typename AddedColumns>
    static size_t switchJoinRightColumns(
        Tables tables,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        HashJoin::Type type,
        JoinStuff::JoinUsedFlags & used_flags);

    template <HashJoin::Type TYPE, typename AddedColumns>
    static size_t joinRightColumnsSwitchNullability(
        Tables tables,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        JoinStuff::JoinUsedFlags & used_flags);

    template <HashJoin::Type TYPE, bool need_filter, typename AddedColumns>
    static size_t joinRightColumnsSwitchMultipleDisjuncts(
        Tables tables,
        AddedColumns & added_columns,
        const ScatteredBlock::Selector & selector,
        JoinStuff::JoinUsedFlags & used_flags);

    /// Joins right table columns which indexes are present in right_indexes using specified map.
    /// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
    /// `fast_path` compiles out the per-row null-map and join-mask checks for the common case of
    /// non-nullable keys and no ON-section condition (the checks are done at runtime otherwise).
    /// With `flag_per_row`, `tables` holds one table per ON disjunct and the used flags are per right row. Without it, there is one table.
    template <HashJoin::Type TYPE, bool need_filter, bool fast_path, bool flag_per_row, typename AddedColumns>
    static size_t joinRightColumns(
        std::conditional_t<flag_per_row, Tables, const HashJoinTable &> tables,
        AddedColumns & added_columns,
        JoinStuff::JoinUsedFlags & used_flags,
        const ScatteredBlock::Selector::Range & selector);

    /// First to collect all matched rows refs by join keys, then filter out rows which are not true in additional filter expression.
    template <HashJoin::Type TYPE, typename AddedColumns>
    static size_t joinRightColumnsWithAdditionalFilter(
        Tables tables,
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
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Semi, HashJoin::MapsSet>;
extern template class HashJoinMethods<JoinKind::Left, JoinStrictness::Anti, HashJoin::MapsSet>;

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
