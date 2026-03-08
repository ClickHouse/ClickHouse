#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/**
 * Implements the `functions_stats` system table, which shows per-function
 * call counts and rows processed for introspection.
 *
 * Useful for identifying expensive or frequently called functions,
 * such as detecting abuse of simpleJSONExtractString or similar.
 */
class StorageSystemFunctionsStats final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemFunctionsStats"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
