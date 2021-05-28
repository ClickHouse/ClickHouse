#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system table 'clusters'
  *  that allows to obtain information about available clusters
  *  (which may be specified in Distributed tables).
  */
class StorageSystemClusters final : public ext::shared_ptr_helper<StorageSystemClusters>, public IStorageSystemOneBlock<StorageSystemClusters>
{
    friend struct ext::shared_ptr_helper<StorageSystemClusters>;
public:
    std::string getName() const override { return "SystemClusters"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
