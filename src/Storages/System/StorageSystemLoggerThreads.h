#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Information about macros for introspection.
  */
class StorageSystemLoggerThreads final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemLoggerThreads"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
