#pragma once
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

    explicit JoinOnKeyColumns(const Block & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_);

    bool isRowFiltered(size_t i) const { return join_mask_column.isRowFiltered(i); }
};

template <bool lazy>
class AddedColumns
{
public:
    struct TypeAndName
    {
        DataTypePtr type;
        String name;
        String qualified_name;

        TypeAndName(DataTypePtr type_, const String & name_, const String & qualified_name_)
            : type(type_), name(name_), qualified_name(qualified_name_)
        {
        }
    };

    struct LazyOutput
    {
        PaddedPODArray<UInt64> row_refs;
    };

    AddedColumns(
        const Block & left_block_,
        const Block & block_with_columns_to_add,
        const Block & saved_block_sample,
        const HashJoin & join,
        std::vector<JoinOnKeyColumns> && join_on_keys_,
        ExpressionActionsPtr additional_filter_expression_,
        bool is_asof_join,
        bool is_join_get_)
        : left_block(left_block_)
        , join_on_keys(join_on_keys_)
        , additional_filter_expression(additional_filter_expression_)
        , rows_to_add(left_block.rows())
        , join_data_avg_perkey_rows(join.getJoinedData()->avgPerKeyRows())
        , output_by_row_list_threshold(join.getTableJoin().outputByRowListPerkeyRowsThreshold())
        , join_data_sorted(join.getJoinedData()->sorted)
        , is_join_get(is_join_get_)
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        if constexpr (lazy)
        {
            has_columns_to_add = num_columns_to_add > 0;
            lazy_output.row_refs.reserve(rows_to_add);
        }

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Column names `src_column.name` and `qualified_name` can differ for StorageJoin,
            /// because it uses not qualified right block column names
            auto qualified_name = join.getTableJoin().renamedRightColumnName(src_column.name);
            /// Don't insert column if it's in left block
            if (!left_block.has(qualified_name))
                addColumn(src_column, qualified_name);
        }

        if (is_asof_join)
        {
            assert(join_on_keys.size() == 1);
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column, right_asof_column.name);
            left_asof_key = join_on_keys[0].key_columns.back();
        }

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.name));

        nullable_column_ptrs.resize(right_indexes.size(), nullptr);
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            /** If it's joinGetOrNull, we will have nullable columns in result block
              * even if right column is not nullable in storage (saved_block_sample).
              */
            const auto & saved_column = saved_block_sample.getByPosition(right_indexes[j]).column;
            if (columns[j]->isNullable() && !saved_column->isNullable())
                nullable_column_ptrs[j] = typeid_cast<ColumnNullable *>(columns[j].get());
        }
    }

    size_t size() const { return columns.size(); }

    void buildOutput();

    void buildJoinGetOutput();

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].type, type_name[i].qualified_name);
    }

    void appendFromBlock(const RowRef * row_ref, bool has_default);

    void appendDefaultRow();

    void applyLazyDefaults();

    const IColumn & leftAsofKey() const { return *left_asof_key; }

    static constexpr bool isLazy() { return lazy; }

    Block left_block;
    std::vector<JoinOnKeyColumns> join_on_keys;
    ExpressionActionsPtr additional_filter_expression;

    size_t max_joined_block_rows = 0;
    size_t rows_to_add;
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate;
    bool need_filter = false;
    bool output_by_row_list = false;
    size_t join_data_avg_perkey_rows = 0;
    size_t output_by_row_list_threshold = 0;
    bool join_data_sorted = false;
    IColumn::Filter filter;

    void reserve(bool need_replicate)
    {
        if (!max_joined_block_rows)
            return;

        /// Do not allow big allocations when user set max_joined_block_rows to huge value
        size_t reserve_size = std::min<size_t>(max_joined_block_rows, DEFAULT_BLOCK_SIZE * 2);

        if (need_replicate)
            /// Reserve 10% more space for columns, because some rows can be repeated
            reserve_size = static_cast<size_t>(1.1 * reserve_size);

        for (auto & column : columns)
            column->reserve(reserve_size);
    }

private:

    void checkBlock(const Block & block)
    {
        for (size_t j = 0; j < right_indexes.size(); ++j)
        {
            const auto * column_from_block = block.getByPosition(right_indexes[j]).column.get();
            const auto * dest_column = columns[j].get();
            if (auto * nullable_col = nullable_column_ptrs[j])
            {
                if (!is_join_get)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Columns {} and {} can have different nullability only in joinGetOrNull",
                                    dest_column->getName(), column_from_block->getName());
                dest_column = nullable_col->getNestedColumnPtr().get();
            }
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

    MutableColumns columns;
    bool is_join_get;
    std::vector<size_t> right_indexes;
    std::vector<TypeAndName> type_name;
    std::vector<ColumnNullable *> nullable_column_ptrs;
    size_t lazy_defaults_count = 0;

    /// for lazy
    // The default row is represented by an empty RowRef, so that fixed-size blocks can be generated sequentially,
    // default_count cannot represent the position of the row
    LazyOutput lazy_output;
    bool has_columns_to_add;

    /// for ASOF
    const IColumn * left_asof_key = nullptr;


    void addColumn(const ColumnWithTypeAndName & src_column, const std::string & qualified_name)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name, qualified_name);
    }

    /** Build output from the blocks that extract from `RowRef` or `RowRefList`, to avoid block cache miss which may cause performance slow down.
     *  And This problem would happen it we directly build output from `RowRef` or `RowRefList`.
     */
    template<bool from_row_list>
    void buildOutputFromBlocks();
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

}
