#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Information about configurations.
  */
class StorageSystemConfigs final : public IStorageSystemOneBlock<StorageSystemConfigs>
{
public:
    std::string getName() const override { return "SystemConfigs"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
