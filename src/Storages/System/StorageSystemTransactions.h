#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemTransactions final : public IStorageSystemOneBlock<StorageSystemTransactions>, boost::noncopyable
{
public:
    String getName() const override { return "SystemTransactions"; }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases() { return {}; }

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
