#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `instrumentation` system table, which allows you to get information about functions instrumented by XRay.
class StorageSystemInstrumentation final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "StorageSystemInstrumentation"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
