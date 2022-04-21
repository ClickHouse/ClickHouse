#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;


/** System table "contributors" with list of clickhouse contributors
  */
class StorageSystemContributors final : public IStorageSystemOneBlock<StorageSystemContributors>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemContributors> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemContributors>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemContributors(CreatePasskey, TArgs &&... args) : StorageSystemContributors{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemContributors";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
