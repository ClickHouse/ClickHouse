#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;


/** Implements `events` system table, which allows you to obtain information for profiling.
  */
class StorageSystemEvents final : public IStorageSystemOneBlock<StorageSystemEvents>
{
public:
    std::string getName() const override { return "SystemEvents"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
