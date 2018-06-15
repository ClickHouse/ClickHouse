#include "VirtualColumnsProcessor.h"

namespace DB
{

static bool hasColumn(const ColumnsWithTypeAndName & columns, const String & column_name)
{
    for (const auto & column : columns)
    {
        if (column.name == column_name)
            return true;
    }

    return false;
}


static NameAndTypePair tryGetColumn(const ColumnsWithTypeAndName & columns, const String & column_name)
{
    for (const auto & column : columns)
    {
        if (column.name == column_name)
            return {column.name, column.type};
    }

    return {};
}


bool StorageWithVirtualColumns::hasColumnImpl(const ITableDeclaration * table, const String & column_name) const
{
    return DB::hasColumn(virtual_columns, column_name) || table->ITableDeclaration::hasColumn(column_name);
}

NameAndTypePair StorageWithVirtualColumns::getColumnImpl(const ITableDeclaration * table, const String & column_name) const
{
    auto virtual_column = DB::tryGetColumn(virtual_columns, column_name);
    return !virtual_column.name.empty() ? virtual_column : table->ITableDeclaration::getColumn(column_name);
}

VirtualColumnsProcessor StorageWithVirtualColumns::getVirtualColumnsProcessor()
{
    return VirtualColumnsProcessor(virtual_columns);
}


Names VirtualColumnsProcessor::process(const Names & column_names, const std::vector<bool *> & virtual_columns_exists_flag)
{
    Names real_column_names;

    if (!virtual_columns_exists_flag.empty())
    {
        for (size_t i = 0; i < all_virtual_columns.size(); ++i)
            *virtual_columns_exists_flag.at(i) = false;
    }

    for (const String & column_name : column_names)
    {
        ssize_t virtual_column_index = -1;

        for (size_t i = 0; i < all_virtual_columns.size(); ++i)
        {
            if (column_name == all_virtual_columns[i].name)
            {
                virtual_column_index = i;
                break;
            }
        }

        if (virtual_column_index >= 0)
        {
            auto index = static_cast<size_t>(virtual_column_index);
            virtual_columns_mask[index] = 1;
            if (!virtual_columns_exists_flag.empty())
                *virtual_columns_exists_flag.at(index) = true;
        }
        else
        {
            real_column_names.emplace_back(column_name);
        }
    }

    return real_column_names;
}

void VirtualColumnsProcessor::appendVirtualColumns(Block & block)
{
    for (size_t i = 0; i < all_virtual_columns.size(); ++i)
    {
        if (virtual_columns_mask[i])
            block.insert(all_virtual_columns[i].cloneEmpty());
    }
}

}
