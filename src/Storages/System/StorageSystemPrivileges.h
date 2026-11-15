#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `privileges` system table, which allows you to get information about access types.
class StorageSystemPrivileges final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemPrivileges"; }
    static ColumnsDescription getColumnsDescription();
    static const std::vector<std::pair<String, Int16>> & getAccessTypeEnumValues();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
