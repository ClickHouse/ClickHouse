#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system.password_rules table that contains complexity rules for passwords for users
  * to be applied in clickhouse-client.
  */
class StorageSystemPasswordRules final : public IStorageSystemOneBlock<StorageSystemPasswordRules>
{
public:
    std::string getName() const override { return "SystemPasswordRules"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const override;
};
}
