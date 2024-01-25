#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/**
 * Implements the `errors` system table, which shows the error code and the number of times it happens
 * (i.e. Exception with this code had been thrown).
 */
class StorageSystemErrors final : public IStorageSystemOneBlock<StorageSystemErrors>
{
public:
    std::string getName() const override { return "SystemErrors"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const override;
};

}
