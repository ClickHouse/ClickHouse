#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `processes` system table, which allows you to get information about the queries that are currently executing.
  */
class StorageSystemUserProcesses final : public IStorageSystemOneBlock<StorageSystemUserProcesses>
{
public:
    std::string getName() const override { return "SystemUserProcesses"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
