#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/** Implements system.fail_points table, which lists all available failpoints
  * and their current status (enabled/disabled).
  * Only available in debug builds (when NDEBUG is not defined).
  */
class StorageSystemFailPoints final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask)
        const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemFailPoints"; }

    static ColumnsDescription getColumnsDescription();
};

}
