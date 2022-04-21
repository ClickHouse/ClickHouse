#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemParts final : public StorageSystemPartsBase
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemParts> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemParts>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemParts(CreatePasskey, TArgs &&... args) : StorageSystemParts{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemParts"; }

protected:
    explicit StorageSystemParts(const StorageID & table_id_);
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
