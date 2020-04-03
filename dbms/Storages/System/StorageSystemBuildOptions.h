#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** System table "build_options" with many params used for clickhouse building
  */
class StorageSystemBuildOptions final : public ext::shared_ptr_helper<StorageSystemBuildOptions>, public IStorageSystemOneBlock<StorageSystemBuildOptions>
{
    friend struct ext::shared_ptr_helper<StorageSystemBuildOptions>;
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:

    std::string getName() const override { return "SystemBuildOptions"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
