#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `databases` system table, which allows you to get information about all databases.
  */
class StorageSystemDatabases final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override
    {
        return "SystemDatabases";
    }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    bool supportsColumnsMask() const override { return true; }

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const override;
    Block getFilterSampleBlock() const override;
};

}
