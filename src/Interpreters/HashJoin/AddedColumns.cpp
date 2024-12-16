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

template<> void AddedColumns<false>::buildOutput()
{
}

template<>
void AddedColumns<true>::buildOutput()
{
    for (size_t i = 0; i < this->size(); ++i)
    {
        auto& col = columns[i];
        size_t default_count = 0;
        auto apply_default = [&]()
        {
            if (default_count > 0)
            {
                JoinCommon::addDefaultValues(*col, type_name[i].type, default_count);
                default_count = 0;
            }
        };

        for (size_t j = 0; j < lazy_output.blocks.size(); ++j)
        {
            if (!lazy_output.blocks[j])
            {
                default_count++;
                continue;
            }
            apply_default();
            const auto & column_from_block = reinterpret_cast<const Block *>(lazy_output.blocks[j])->getByPosition(right_indexes[i]);
            /// If it's joinGetOrNull, we need to wrap not-nullable columns in StorageJoin.
            if (is_join_get)
            {
                if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get());
                    nullable_col && !column_from_block.column->isNullable())
                {
                    nullable_col->insertFromNotNullable(*column_from_block.column, lazy_output.row_nums[j]);
                    continue;
                }
            }
            col->insertFrom(*column_from_block.column, lazy_output.row_nums[j]);
        }
        apply_default();
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
void AddedColumns<true>::applyLazyDefaults()
{
}

template <>
void AddedColumns<false>::appendFromBlock(const Block & block, size_t row_num,const bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

#ifndef NDEBUG
    checkBlock(block);
#endif
    if (is_join_get)
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = block.getByPosition(right_indexes[j]);
            if (auto * nullable_col = nullable_column_ptrs[j])
                nullable_col->insertFromNotNullable(*column_from_block.column, row_num);
            else
                columns[j]->insertFrom(*column_from_block.column, row_num);
        }
    }
    else
    {
        size_t right_indexes_size = right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = block.getByPosition(right_indexes[j]);
            columns[j]->insertFrom(*column_from_block.column, row_num);
        }
    }
}

template <>
void AddedColumns<true>::appendFromBlock(const Block & block, size_t row_num, bool)
{
#ifndef NDEBUG
    checkBlock(block);
#endif
    if (has_columns_to_add)
    {
        lazy_output.blocks.emplace_back(reinterpret_cast<UInt64>(&block));
        lazy_output.row_nums.emplace_back(static_cast<uint32_t>(row_num));
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
        lazy_output.blocks.emplace_back(0);
        lazy_output.row_nums.emplace_back(0);
    }
}
}
