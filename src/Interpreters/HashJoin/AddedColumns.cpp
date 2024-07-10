#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/NullableUtils.h>

namespace DB
{
JoinOnKeyColumns::JoinOnKeyColumns(const Block & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_)
    : key_names(key_names_)
    , materialized_keys_holder(JoinCommon::materializeColumns(
          block, key_names)) /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    , key_columns(JoinCommon::getRawPointers(materialized_keys_holder))
    , null_map(nullptr)
    , null_map_holder(extractNestedColumnsAndNullMap(key_columns, null_map))
    , join_mask_column(JoinCommon::getColumnAsMask(block, cond_column_name))
    , key_sizes(key_sizes_)
{
}

template<> void AddedColumns<false>::buildOutputFromRowRef() {}

template<> void AddedColumns<false>::buildOutputFromRowRefList() {}

template<> void AddedColumns<false>::buildOutputFromSortedRowRefList() {}

template<> void AddedColumns<false>::buildJoinGetOutput() {}

template<> void AddedColumns<true>::buildOutputFromRowRef()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        for (auto row_ref_i : lazy_output.row_refs)
        {
            if (row_ref_i)
            {
                const RowRef * row_ref = reinterpret_cast<const RowRef *>(row_ref_i);
                col->insertFrom(*row_ref->block->getByPosition(right_indexes[i]).column, row_ref->row_num);
            }
            else
                type_name[i].type->insertDefaultInto(*col);
        }
    }
}

template<> void AddedColumns<true>::buildOutputFromRowRefList()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        for (auto row_ref_i : lazy_output.row_refs)
        {
            if (row_ref_i)
            {
                const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(row_ref_i);
                for (auto it = row_ref_list->begin(); it.ok(); ++it)
                    col->insertFrom(*it->block->getByPosition(right_indexes[i]).column, it->row_num);
            }
            else
                type_name[i].type->insertDefaultInto(*col);
        }
    }
}

template<> void AddedColumns<true>::buildOutputFromSortedRowRefList()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        for (auto row_ref_i : lazy_output.row_refs)
        {
            if (row_ref_i)
            {
                const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(row_ref_i);
                col->insertRangeFrom(*row_ref_list->block->getByPosition(right_indexes[i]).column, row_ref_list->row_num, row_ref_list->rows);
            }
            else
                type_name[i].type->insertDefaultInto(*col);
        }
    }
}


template<> void AddedColumns<true>::buildJoinGetOutput()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto & col = columns[i];
        for (auto row_ref_i : lazy_output.row_refs)
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
void AddedColumns<true>::appendFromBlock(const RowRef * row_ref, bool)
{
#ifndef NDEBUG
    checkBlock(*row_ref->block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.row_refs.emplace_back(reinterpret_cast<UInt64>(row_ref));
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
        lazy_output.row_refs.emplace_back(0);
    }
}
}
