#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

/// Introspection contract for durable UDT definitions. Reserved semantic
/// columns keep the table shape stable as additional capabilities are enabled.
class StorageSystemUDTs final : public IStorageSystemOneBlock
{
public:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    std::string getName() const override { return "SystemUDTs"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
