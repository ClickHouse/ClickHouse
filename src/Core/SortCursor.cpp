#include <Core/Block.h>
#include <Core/SortCursor.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

void SortCursorImpl::reset(const Block & block, IColumnPermutation * perm)
{
    if (block.getColumns().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty column list in block");
    reset(block.getColumns(), block, block.getColumns()[0]->size(), perm);
}

void SortCursorImpl::reset(const Columns & columns, const Block & block, UInt64 num_rows, IColumnPermutation * perm)
{
    all_columns.clear();
    sort_columns.clear();
#if USE_EMBEDDED_COMPILER
    raw_sort_columns_data.clear();
#endif

    size_t num_columns = columns.size();

    for (size_t j = 0; j < num_columns; ++j)
        all_columns.push_back(columns[j].get());

    for (size_t j = 0, size = desc.size(); j < size; ++j)
    {
        auto & column_desc = desc[j];
        size_t column_number = block.getPositionByName(column_desc.column_name);
        sort_columns.push_back(columns[column_number].get());

#if USE_EMBEDDED_COMPILER
        if (desc.compiled_sort_description)
            raw_sort_columns_data.emplace_back(getColumnData(sort_columns.back()));
#endif
        need_collation[j] = desc[j].collator != nullptr && sort_columns.back()->isCollationSupported();
        has_collation |= need_collation[j];
    }

    pos = 0;
    rows = num_rows;
    permutation = perm;
}

DataTypes SortQueueVariants::extractSortDescriptionTypesFromHeader(const Block & header, const SortDescription & sort_description)
{
    size_t sort_description_size = sort_description.size();
    DataTypes data_types(sort_description_size);

    for (size_t i = 0; i < sort_description_size; ++i)
    {
        const auto & column_sort_description = sort_description[i];
        data_types[i] = header.getByName(column_sort_description.column_name).type;
    }

    return data_types;
}

}
