#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/** System table "time_zones" with list of timezones pulled from /contrib/cctz/testdata/zoneinfo
  */
class StorageSystemTimeZones final : public IStorageSystemOneBlock
{
public:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    std::string getName() const override { return "SystemTimeZones"; }

    static ColumnsDescription getColumnsDescription();
};
}
