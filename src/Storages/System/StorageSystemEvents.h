#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class Context;


/** Implements `events` system table, which allows you to obtain information for profiling.
  */
class StorageSystemEvents final : public IStorageSystemOneBlock<StorageSystemEvents>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemEvents> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemEvents>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemEvents(CreatePasskey, TArgs &&... args) : StorageSystemEvents{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemEvents"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
