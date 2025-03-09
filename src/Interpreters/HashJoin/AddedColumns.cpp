#include <Interpreters/HashJoin/AddedColumns.h>
#include <DataTypes/NullableUtils.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JoinOnKeyColumns::JoinOnKeyColumns(
    const ScatteredBlock & block_, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_)
    : block(block_)
    , key_names(key_names_)
    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    , materialized_keys_holder(JoinCommon::materializeColumns(block.getSourceBlock(), key_names))
    , key_columns(JoinCommon::getRawPointers(materialized_keys_holder))
    , null_map(nullptr)
    , null_map_holder(extractNestedColumnsAndNullMap(key_columns, null_map))
    , join_mask_column(JoinCommon::getColumnAsMask(block.getSourceBlock(), cond_column_name))
    , key_sizes(key_sizes_)
{
}

template<>
void AddedColumns<false>::buildOutput() {}

template<>
void AddedColumns<false>::buildJoinGetOutput() {}

template<>
template<bool from_row_list>
void AddedColumns<false>::buildOutputFromBlocks() {}

template<>
void AddedColumns<true>::buildOutput()
{
    if (!output_by_row_list)
        buildOutputFromBlocks<false>();
    else
    {
        if (join_data_avg_perkey_rows < output_by_row_list_threshold)
            buildOutputFromBlocks<true>();
        else if (join_data_sorted)
            buildOutputFromRowRefLists<true>();
        else
            buildOutputFromRowRefLists<false>();
    }
}

/// Fills column values from RowRefList
/// Implementation with concrete column type allows to de-virtualize col->insertFrom() calls
template <bool join_data_sorted, typename ColumnType>
static void fillTypedColumn(ColumnType * col, const size_t right_index, const DataTypePtr & type, const size_t output_row_count, const PaddedPODArray<UInt64> & row_refs)
{
    col->reserve(col->size() + output_row_count);
    for (auto row_ref_i : row_refs)
    {
        if (row_ref_i)
        {
            const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(row_ref_i);
            if constexpr (join_data_sorted)
            {
                col->insertRangeFrom(*row_ref_list->block->getByPosition(right_index).column, row_ref_list->row_num, row_ref_list->rows);
            }
            else
            {
                for (auto it = row_ref_list->begin(); it.ok(); ++it)
                    col->insertFrom(*it->block->getByPosition(right_index).column, it->row_num);
            }
        }
        else
            type->insertDefaultInto(*col);
    }
}

/// Fills column values from RowRefList
template <bool join_data_sorted>
static void fillColumn(const MutableColumnPtr & column, const size_t right_index, const DataTypePtr & type, const size_t output_row_count, const PaddedPODArray<UInt64> & row_refs)
{
    bool filled = false;

#define TRY_FILL_TYPED_COLUMN_BY_TYPE_INDEX(column_type_index, column_type) \
    case TypeIndex::column_type_index : \
    { \
        auto * typed_column = typeid_cast<column_type *>(column.get()); \
        if (typed_column) \
        { \
            fillTypedColumn<join_data_sorted>(typed_column, right_index, type, output_row_count, row_refs); \
            filled = true; \
        } \
        break; \
    }

#define TRY_FILL_TYPED_COLUMN(column_type_index) \
    TRY_FILL_TYPED_COLUMN_BY_TYPE_INDEX(column_type_index, Column ## column_type_index)


    switch (column->getDataType())
    {
    TRY_FILL_TYPED_COLUMN(UInt8)
    TRY_FILL_TYPED_COLUMN(UInt16)
    TRY_FILL_TYPED_COLUMN(UInt32)
    TRY_FILL_TYPED_COLUMN(UInt64)
    TRY_FILL_TYPED_COLUMN(Int8)
    TRY_FILL_TYPED_COLUMN(Int16)
    TRY_FILL_TYPED_COLUMN(Int32)
    TRY_FILL_TYPED_COLUMN(Int64)
    TRY_FILL_TYPED_COLUMN(Float32)
    TRY_FILL_TYPED_COLUMN(Float64)
    TRY_FILL_TYPED_COLUMN(BFloat16)
    TRY_FILL_TYPED_COLUMN(UUID)
    TRY_FILL_TYPED_COLUMN(IPv4)
    TRY_FILL_TYPED_COLUMN(IPv6)
    TRY_FILL_TYPED_COLUMN_BY_TYPE_INDEX(Decimal32, ColumnDecimal<Decimal32>)
    TRY_FILL_TYPED_COLUMN_BY_TYPE_INDEX(Decimal64, ColumnDecimal<Decimal64>)
    TRY_FILL_TYPED_COLUMN_BY_TYPE_INDEX(DateTime64, ColumnDecimal<DateTime64>)
    TRY_FILL_TYPED_COLUMN(String)
    TRY_FILL_TYPED_COLUMN(FixedString)
    default:
    }

#undef TRY_FILL_TYPED_COLUMN_BY_TYPE_INDEX
#undef TRY_FILL_TYPED_COLUMN

    /// Generic implementation for IColumn
    if (!filled)
        fillTypedColumn<join_data_sorted>(column.get(), right_index, type, output_row_count, row_refs);
}

template<>
template<bool join_data_sorted>
void AddedColumns<true>::buildOutputFromRowRefLists()
{
    const size_t output_row_count = lazy_output.getRowCount();

    for (size_t i = 0; i < this->size(); ++i)
        fillColumn<join_data_sorted>(columns[i], right_indexes[i], type_name[i].type, output_row_count, lazy_output.getRowRefs());
}

template<>
void AddedColumns<true>::buildJoinGetOutput()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        for (auto row_ref_i : lazy_output.getRowRefs())
        {
            if (!row_ref_i)
            {
                type_name[i].type->insertDefaultInto(*col);
                continue;
            }
            const auto * row_ref = reinterpret_cast<const RowRef *>(row_ref_i);
            const auto & column_from_block = row_ref->block->getByPosition(right_indexes[i]);
            if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get()); nullable_col && !column_from_block.column->isNullable())
                nullable_col->insertFromNotNullable(*column_from_block.column, row_ref->row_num);
            else
                col->insertFrom(*column_from_block.column, row_ref->row_num);
        }
    }
}

template<>
template<bool from_row_list>
void AddedColumns<true>::buildOutputFromBlocks()
{
    if (this->size() == 0)
        return;
    std::vector<const Block *> blocks;
    std::vector<UInt32> row_nums;
    blocks.reserve(lazy_output.getRowCount());
    row_nums.reserve(lazy_output.getRowCount());
    for (auto row_ref_i : lazy_output.getRowRefs())
    {
        if (row_ref_i)
        {
            if constexpr (from_row_list)
            {
                const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(row_ref_i);
                for (auto it = row_ref_list->begin(); it.ok(); ++it)
                {
                    blocks.emplace_back(it->block);
                    row_nums.emplace_back(it->row_num);
                }
            }
            else
            {
                const RowRef * row_ref = reinterpret_cast<const RowRefList *>(row_ref_i);
                blocks.emplace_back(row_ref->block);
                row_nums.emplace_back(row_ref->row_num);
            }
        }
        else
        {
            blocks.emplace_back(nullptr);
            row_nums.emplace_back(0);
        }
    }
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        col->reserve(col->size() + blocks.size());
        for (size_t j = 0; j < blocks.size(); ++j)
        {
            if (blocks[j])
                col->insertFrom(*blocks[j]->getByPosition(right_indexes[i]).column, row_nums[j]);
            else
                type_name[i].type->insertDefaultInto(*col);
        }
    }
}

template<>
void AddedColumns<false>::applyLazyDefaults()
{
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            JoinCommon::addDefaultValues(*columns[j], type_name[j].type, lazy_defaults_count);
        lazy_defaults_count = 0;
    }
}

template<>
void AddedColumns<true>::applyLazyDefaults() {}

template <>
void AddedColumns<false>::appendFromBlock(const RowRef * row_ref, const bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

#ifndef NDEBUG
    checkBlock(*row_ref->block);
#endif
    if (is_join_get)
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = row_ref->block->getByPosition(right_indexes[j]);
            if (auto * nullable_col = nullable_column_ptrs[j])
                nullable_col->insertFromNotNullable(*column_from_block.column, row_ref->row_num);
            else
                columns[j]->insertFrom(*column_from_block.column, row_ref->row_num);
        }
    }
    else
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = row_ref->block->getByPosition(right_indexes[j]);
            columns[j]->insertFrom(*column_from_block.column, row_ref->row_num);
        }
    }
}

template <>
__attribute__((noreturn)) void AddedColumns<false>::appendFromBlock(const RowRefList *, bool)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "AddedColumns are not implemented for RowRefList in non-lazy mode");
}


template <>
void AddedColumns<true>::appendFromBlock(const RowRef * row_ref, bool)
{
#ifndef NDEBUG
    checkBlock(*row_ref->block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.addRowRef(row_ref);
    }
}

template <>
void AddedColumns<true>::appendFromBlock(const RowRefList * row_ref_list, bool)
{
#ifndef NDEBUG
    checkBlock(*row_ref_list->block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.addRowRefList(row_ref_list);
    }
}


template<>
void AddedColumns<false>::appendDefaultRow()
{
    ++lazy_defaults_count;
}

template<>
void AddedColumns<true>::appendDefaultRow()
{
    if (has_columns_to_add)
    {
        lazy_output.addDefault();
    }
}
}
