#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `processes` system table, which allows you to get information about the queries that are currently executing.
  */
class StorageSystemProcesses final : public shared_ptr_helper<StorageSystemProcesses>, public IStorageSystemOneBlock<StorageSystemProcesses>
{
    friend struct shared_ptr_helper<StorageSystemProcesses>;
public:
    std::string getName() const override { return "SystemProcesses"; }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
