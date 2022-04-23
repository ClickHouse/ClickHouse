#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system.warnings table that contains warnings about server configuration
  * to be displayed in clickhouse-client.
  */
class StorageSystemWarnings final : public IStorageSystemOneBlock<StorageSystemWarnings>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemWarnings> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemWarnings>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemWarnings(CreatePasskey, TArgs &&... args) : StorageSystemWarnings{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemWarnings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const override;
};
}
