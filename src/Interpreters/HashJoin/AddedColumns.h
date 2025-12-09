#pragma once

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Core/Defines.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/TableJoin.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct JoinOnKeyColumns
{
    Names key_names;

    Columns materialized_keys_holder;
    ColumnRawPtrs key_columns;

    ConstNullMapPtr null_map;
    ColumnPtr null_map_holder;

    /// Only rows where mask == true can be joined
    JoinCommon::JoinMask join_mask_column;

    Sizes key_sizes;

    JoinOnKeyColumns(
        const ScatteredBlock & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_);

    bool isRowFiltered(size_t i) const
    {
        return join_mask_column.isRowFiltered(i);
    }
};

struct LazyOutput
{
    PaddedPODArray<UInt64> row_refs;
    size_t row_count = 0;   /// Total number of rows in all RowRef-s and RowRefList-s

    std::vector<size_t> right_indexes;
    NamesAndTypes type_name;

    bool join_data_sorted = false;
    bool output_by_row_list = false;
    size_t output_by_row_list_threshold = 0;
    size_t join_data_avg_perkey_rows = 0;

    const PaddedPODArray<UInt64> & getRowRefs() const { return row_refs; }
    size_t getRowCount() const { return row_count; }

    void reserve(size_t size) { row_refs.reserve(size); }

    void addRowRef(const RowRef * row_ref)
    {
        row_refs.emplace_back(reinterpret_cast<UInt64>(row_ref));
        ++row_count;
    }

    void addRowRefList(const RowRefList * row_ref_list)
    {
        row_refs.emplace_back(reinterpret_cast<UInt64>(row_ref_list));
        row_count += row_ref_list->rows;
    }

    void addDefault()
    {
        row_refs.emplace_back(0);
        ++row_count;
    }

    [[nodiscard]] size_t buildOutput(
        size_t size_to_reserve,
        const Block & left_block,
        const IColumn::Offsets & left_offsets,
        MutableColumns & columns,
        const UInt64 * row_refs_begin,
        const UInt64 * row_refs_end,
        size_t rows_offset,
        size_t rows_limit,
        size_t bytes_limit) const;

    void buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const;

    /** Build output from the blocks that extract from `RowRef` or `RowRefList`, to avoid block cache miss which may cause performance slow down.
     *  And This problem would happen it we directly build output from `RowRef` or `RowRefList`.
     */
    template<bool from_row_list>
    void buildOutputFromBlocks(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const;

    void buildOutputFromRowRefLists(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const;

    [[nodiscard]] size_t buildOutputFromBlocksLimitAndOffset(
        MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end,
        const PaddedPODArray<UInt64> & left_sizes, const IColumn::Offsets & left_offsets,
        size_t rows_offset, size_t rows_limit, size_t bytes_limit) const;

private:
};

template <bool lazy>
class AddedColumns
{
public:

    AddedColumns(
        const ScatteredBlock & left_block_,
        const Block & block_with_columns_to_add,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        ExpressionActionsPtr additional_filter_expression_,
        const std::vector<std::pair<size_t, size_t>> & additional_filter_required_rhs_pos_,
        bool is_asof_join,
        bool is_join_get_)
        : left_block(left_block_.getSourceBlock())
        , join_on_keys(join_on_keys_)
        , additional_filter_expression(additional_filter_expression_)
        , additional_filter_required_rhs_pos(additional_filter_required_rhs_pos_)
        , rows_to_add(left_block_.rows())
        , is_join_get(is_join_get_)
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        if constexpr (lazy)
        {
            has_columns_to_add = num_columns_to_add > 0;
            lazy_output.reserve(rows_to_add);
        }

        columns.reserve(num_columns_to_add);
        lazy_output.type_name.reserve(num_columns_to_add);
        lazy_output.right_indexes.reserve(num_columns_to_add);

        lazy_output.output_by_row_list_threshold = join.getTableJoin().outputByRowListPerkeyRowsThreshold();
        lazy_output.join_data_sorted = join.getJoinedData()->sorted;
        lazy_output.join_data_avg_perkey_rows = join.getJoinedData()->avgPerKeyRows();

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
            /// because it uses not qualified right block column names
            auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
            /// Don't insert column if it's in left block
            if (!left_block.has(qualified_name))
                addColumn(src_column);
        }

        if (is_asof_join)
        {
            assert(join_on_keys.size() == 1);
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column);
            left_asof_key = join_on_keys[0].key_columns.back();
        }

        for (auto & tn : lazy_output.type_name)
            lazy_output.right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));

        nullable_column_ptrs.resize(lazy_output.right_indexes.size(), nullptr);
        for (size_t j = 0; j < lazy_output.right_indexes.size(); ++j)
        {
            /** If it's joinGetOrNull, we will have nullable columns in result block
              * even if right column is not nullable in storage (saved_block_sample).
              */
            const auto & saved_column = saved_block_sample.getByPosition(lazy_output.right_indexes[j]).column;
            if (columns[j]->isNullable() && !saved_column->isNullable())
                nullable_column_ptrs[j] = typeid_cast<ColumnNullable *>(columns[j].get());
        }
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), lazy_output.type_name[i].type, lazy_output.type_name[i].name);
    }

    void appendFromBlock(const RowRefList * row_ref_list, bool)
    {
        if constexpr (lazy)
        {
#ifndef NDEBUG
            checkColumns(row_ref_list->columns_info->columns);
#endif
            if (has_columns_to_add)
            {
                lazy_output.addRowRefList(row_ref_list);
            }
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "AddedColumns are not implemented for RowRefList in non-lazy mode");
        }
    }

    void appendFromBlock(const RowRef * row_ref, bool has_default);

    void appendDefaultRow()
    {
        if constexpr (!lazy)
        {
            ++lazy_defaults_count;
        }
        else
        {
            if (has_columns_to_add)
                lazy_output.addDefault();
        }
    }

    void applyLazyDefaults();

    const IColumn & leftAsofKey() const { return *left_asof_key; }

    static constexpr bool isLazy() { return lazy; }

    Block left_block;
    std::vector<JoinOnKeyColumns> join_on_keys;
    ExpressionActionsPtr additional_filter_expression;
    const std::vector<std::pair<size_t, size_t>> & additional_filter_required_rhs_pos;

    size_t max_joined_block_rows = 0;
    size_t rows_to_add;
    bool need_filter = false;

    MutableColumns columns;
    IColumn::Offsets offsets_to_replicate;
    IColumn::Filter filter;
    /// For every row with a match, if we set filter[row] = 1, we also add this row to `matched_rows` for faster ScatteredBlock::filter().
    IColumn::Offsets matched_rows;

    /// for lazy
    // The default row is represented by an empty RowRef, so that fixed-size blocks can be generated sequentially,
    // default_count cannot represent the position of the row
    LazyOutput lazy_output;
    bool has_columns_to_add;

    void reserve(bool need_replicate)
    {
        /// If lazy, we will reserve right after actual insertion into columns, because at that moment we will know the exact number of rows to add.
        if constexpr (lazy)
            return;

        if (!max_joined_block_rows)
            return;

        /// Do not allow big allocations when user set max_joined_block_rows to huge value
        size_t reserve_size = std::min<size_t>(max_joined_block_rows, rows_to_add * 2);

        if (need_replicate)
            /// Reserve 10% more space for columns, because some rows can be repeated
            reserve_size = static_cast<size_t>(1.1 * reserve_size);

        for (auto & column : columns)
            column->reserve(reserve_size);
    }

private:

    void checkColumns(const Columns & to_check)
    {
        for (size_t j = 0; j < lazy_output.right_indexes.size(); ++j)
        {
            const auto * column_from_block = to_check.at(lazy_output.right_indexes[j]).get();
            const auto * dest_column = columns[j].get();
            if (auto * nullable_col = nullable_column_ptrs[j])
            {
                if (!is_join_get)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Columns {} and {} can have different nullability only in joinGetOrNull",
                                    dest_column->getName(), column_from_block->getName());
                dest_column = nullable_col->getNestedColumnPtr().get();
            }

            if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(column_from_block))
                column_from_block = column_replicated->getNestedColumn().get();
            if (const auto * column_replicated = typeid_cast<const ColumnReplicated *>(dest_column))
                dest_column = column_replicated->getNestedColumn().get();

            /** Using dest_column->structureEquals(*column_from_block) will not work for low cardinality columns,
              * because dictionaries can be different, while calling insertFrom on them is safe, for example:
              * ColumnLowCardinality(size = 0, UInt8(size = 0), ColumnUnique(size = 1, String(size = 1)))
              * and
              * ColumnLowCardinality(size = 0, UInt16(size = 0), ColumnUnique(size = 1, String(size = 1)))
              */
            if (typeid(*dest_column) != typeid(*column_from_block))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Columns {} and {} have different types {} and {}",
                                dest_column->getName(), column_from_block->getName(),
                                demangle(typeid(*dest_column).name()), demangle(typeid(*column_from_block).name()));
        }
    }

    bool is_join_get;
    std::vector<ColumnNullable *> nullable_column_ptrs;
    size_t lazy_defaults_count = 0;

    /// for ASOF
    const IColumn * left_asof_key = nullptr;

    void addColumn(const ColumnWithTypeAndName & src_column)
    {
        columns.push_back(src_column.column->cloneEmpty());
        /// If lazy, we will reserve right after actual insertion into columns, because at that moment we will know the exact number of rows to add.
        if constexpr (!lazy)
            columns.back()->reserve(rows_to_add);
        lazy_output.type_name.emplace_back(src_column.name, src_column.type);
    }
};

/// Adapter class to pass into addFoundRowAll
/// In joinRightColumnsWithAdditionalFilter we don't want to add rows directly into AddedColumns,
/// because they need to be filtered by additional_filter_expression.
class PreSelectedRows : public std::vector<const RowRef *>
{
public:
    void appendFromBlock(const RowRef * row_ref, bool /* has_default */) { this->emplace_back(row_ref); }
    static constexpr bool isLazy() { return false; }
};

std::pair<const IColumn *, size_t> getBlockColumnAndRow(const RowRef * row_ref, size_t column_index);

}
