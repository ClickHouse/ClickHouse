#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `user_defined_functions` system table, which provides information about
  * executable user-defined functions including their configuration, loading status,
  * and execution parameters.
  */
class StorageSystemUserDefinedFunctions final : public IStorageSystemOneBlock
{
public:
    StorageSystemUserDefinedFunctions(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemUserDefinedFunctions"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
