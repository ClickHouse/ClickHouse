#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;

/** Implements system table 'user_defined_types'.
 *  This table shows all user-defined types that are currently registered in the system.
 */
class StorageSystemUserDefinedTypes final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemUserDefinedTypes"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
