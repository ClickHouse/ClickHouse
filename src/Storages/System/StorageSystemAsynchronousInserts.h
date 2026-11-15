#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/** Implements the system table `asynhronous_inserts`,
 *  which contains information about pending asynchronous inserts in queue.
*/
class StorageSystemAsynchronousInserts final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemAsynchronousInserts"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
