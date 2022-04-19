#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** System table "build_options" with many params used for clickhouse building
  */
class StorageSystemBuildOptions final : public IStorageSystemOneBlock<StorageSystemBuildOptions>, boost::noncopyable
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:

    std::string getName() const override { return "SystemBuildOptions"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
