#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `instrumentation` system table, which allows you to get information about functions instrumented by XRay.
class SystemStorageXRayInstrumentation final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemStorageInstrumentation"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
