#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** System table that lists data and delete files of currently loaded Iceberg tables.
  * Each row corresponds to one file referenced by the current snapshot's manifest entries.
  */
class StorageSystemIcebergFiles final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemIcebergFiles"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
