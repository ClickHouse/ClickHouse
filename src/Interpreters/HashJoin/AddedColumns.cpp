#include <Interpreters/HashJoin/AddedColumns.h>
#include <DataTypes/NullableUtils.h>

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
void AddedColumns<true>::buildOutputFromRowRefLists();

template<>
void AddedColumns<true>::buildOutput()
{
    if (!output_by_row_list)
        buildOutputFromBlocks<false>();
    else
    {
        if (join_data_avg_perkey_rows < output_by_row_list_threshold)
            buildOutputFromBlocks<true>();
        else
            buildOutputFromRowRefLists();
    }
}

template<>
void AddedColumns<true>::buildOutputFromRowRefLists()
{
    const size_t output_row_count = lazy_output.getRowCount();

    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        col->reserve(col->size() + output_row_count);
        col->fillFromRowRefs(type_name[i].type, right_indexes[i], lazy_output.getRowRefs(), join_data_sorted);
    }
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
        columns[i]->fillFromBlocksAndRowNumbers(type_name[i].type, right_indexes[i], blocks, row_nums);
    }
}

template<>
size_t AddedColumns<false>::applyLazyDefaults()
{
    size_t added_bytes = 0;
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
        {
            size_t bytes_before = max_joined_block_bytes ? columns[j]->allocatedBytes() : 0;
            JoinCommon::addDefaultValues(*columns[j], type_name[j].type, lazy_defaults_count);
            size_t bytes_after = max_joined_block_bytes ? columns[j]->allocatedBytes() : 0;
            added_bytes += bytes_after - bytes_before;
        }
        lazy_defaults_count = 0;
    }

    return added_bytes;
}

template<>
size_t AddedColumns<true>::applyLazyDefaults() { return 0; }

template <>
size_t AddedColumns<false>::appendFromBlock(const RowRef * row_ref, const bool has_defaults)
{
    size_t added_bytes = 0;
    if (has_defaults)
        added_bytes += applyLazyDefaults();

    auto insert = [&](auto & src, const IColumn & dst, auto inserter, size_t row)
    {
        size_t bytes_before = max_joined_block_bytes ? src.allocatedBytes() : 0;
        (src.*inserter)(dst, row);
        size_t bytes_after = max_joined_block_bytes ? src.allocatedBytes() : 0;
        added_bytes += bytes_after - bytes_before;
    };

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
                insert(*nullable_col, *column_from_block.column, &ColumnNullable::insertFromNotNullable, row_ref->row_num);
                //nullable_col->insertFromNotNullable(*column_from_block.column, row_ref->row_num);
            else
                insert(*columns[j], *column_from_block.column, &IColumn::insertFrom, row_ref->row_num);
                // columns[j]->insertFrom(*column_from_block.column, row_ref->row_num);
        }
    }
    else
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = row_ref->block->getByPosition(right_indexes[j]);
            insert(*columns[j], *column_from_block.column, &IColumn::insertFrom, row_ref->row_num);
            // columns[j]->insertFrom(*column_from_block.column, row_ref->row_num);
        }
    }

    return added_bytes + avg_left_block_bytes_per_row;
}

template <>
__attribute__((noreturn)) size_t AddedColumns<false>::appendFromBlock(const RowRefList *, bool)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "AddedColumns are not implemented for RowRefList in non-lazy mode");
}


template <>
size_t AddedColumns<true>::appendFromBlock(const RowRef * row_ref, bool)
{
#ifndef NDEBUG
    checkBlock(*row_ref->block);
#endif
    //size_t added_bytes = 0;
    if (has_columns_to_add)
    {
        //added_bytes += avg_left_block_bytes_per_row;
        lazy_output.addRowRef(row_ref);
    }
    return avg_left_block_bytes_per_row; //added_bytes;
}

template <>
size_t AddedColumns<true>::appendFromBlock(const RowRefList * row_ref_list, bool)
{
#ifndef NDEBUG
    checkBlock(*row_ref_list->block);
#endif
    //size_t added_bytes = 0;
    if (has_columns_to_add)
    {
        //added_bytes += avg_left_block_bytes_per_row * row_ref_list->rows;
        lazy_output.addRowRefList(row_ref_list);
    }
    return avg_left_block_bytes_per_row * row_ref_list->rows; //added_bytes;
}


template<>
size_t AddedColumns<false>::appendDefaultRow()
{
    ++lazy_defaults_count;
    return avg_left_block_bytes_per_row;
}

template<>
size_t AddedColumns<true>::appendDefaultRow()
{
    size_t added_bytes = 0;
    if (has_columns_to_add)
    {
        added_bytes += avg_left_block_bytes_per_row;
        lazy_output.addDefault();
    }
    return added_bytes;
}
}
