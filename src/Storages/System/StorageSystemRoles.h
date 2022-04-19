#pragma once

#include <boost/noncopyable.hpp>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `roles` system table, which allows you to get information about roles.
class StorageSystemRoles final : public IStorageSystemOneBlock<StorageSystemRoles>, boost::noncopyable
{
public:
    std::string getName() const override { return "SystemRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
