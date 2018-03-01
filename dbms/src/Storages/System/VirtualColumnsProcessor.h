#pragma once

#include <Common/TypeList.h>
#include <Core/NamesAndTypes.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Storages/IStorage.h>


/// Some virtual columns routines
namespace DB
{

struct VirtualColumnsProcessor;


/// Adaptor for storages having virtual columns
class StorageWithVirtualColumns : public IStorage
{
public:

    bool hasColumn(const String & column_name) const override
    {
        return hasColumnImpl(this, column_name);
    }

    NameAndTypePair getColumn(const String & column_name) const override
    {
        return getColumnImpl(this, column_name);
    }

protected:

    VirtualColumnsProcessor getVirtualColumnsProcessor();

    ColumnsWithTypeAndName virtual_columns;

protected:

    /// Use these methods instead of regular hasColumn / getColumn
    bool hasColumnImpl(const ITableDeclaration * table, const String & column_name) const;

    NameAndTypePair getColumnImpl(const ITableDeclaration * table, const String & column_name) const;
};


struct VirtualColumnsProcessor
{
    explicit VirtualColumnsProcessor(const ColumnsWithTypeAndName & all_virtual_columns_)
        : all_virtual_columns(all_virtual_columns_), virtual_columns_mask(all_virtual_columns_.size(), 0) {}

    /// Separates real and virtual column names, returns real ones
    Names process(const Names & column_names, const std::vector<bool *> & virtual_columns_exists_flag = {});

    /// Append spevified virtual columns (with empty data) to the result block
    void appendVirtualColumns(Block & block);

protected:
    const ColumnsWithTypeAndName & all_virtual_columns;
    std::vector<UInt8> virtual_columns_mask;
};

}
