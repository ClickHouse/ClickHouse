#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemModels final : public IStorageSystemOneBlock<StorageSystemModels>, boost::noncopyable
{
public:
    std::string getName() const override { return "SystemModels"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
