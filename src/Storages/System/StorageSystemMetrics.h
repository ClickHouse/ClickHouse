#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `metrics` system table, which provides information about the operation of the server.
  */
class StorageSystemMetrics final : public IStorageSystemOneBlock<StorageSystemMetrics>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemMetrics> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemMetrics>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemMetrics(CreatePasskey, TArgs &&... args) : StorageSystemMetrics{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemMetrics"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
