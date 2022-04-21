#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/** System table "time_zones" with list of timezones pulled from /contrib/cctz/testdata/zoneinfo
  */
class StorageSystemTimeZones final : public IStorageSystemOneBlock<StorageSystemTimeZones>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemTimeZones> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemTimeZones>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemTimeZones(CreatePasskey, TArgs &&... args) : StorageSystemTimeZones{std::forward<TArgs>(args)...}
    {
    }

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemTimeZones"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
