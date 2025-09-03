#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/** Implements `completions` system table, which lists all unique terms (e.g. database names, table names, functions..),
 * accessible to the current user to power auto-completion.
 */
class StorageSystemCompletions final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override
    {
        return "SystemCompletions";
    }

    static ColumnsDescription getColumnsDescription();
protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const override;
};

}
