#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{

/// Provides information about Graphite configuration.
class StorageSystemGraphite : public ext::shared_ptr_helper<StorageSystemGraphite>, public IStorageSystemOneBlock<StorageSystemGraphite>
{
public:
    std::string getName() const override { return "SystemGraphite"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
