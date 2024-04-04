#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/** System table "time_zones" with list of timezones pulled from /contrib/cctz/testdata/zoneinfo
  */
class StorageSystemTimeZones final : public IStorageSystemOneBlock<StorageSystemTimeZones>
{
public:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemTimeZones"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
