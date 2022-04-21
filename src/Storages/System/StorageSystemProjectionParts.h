#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'projection_parts' which allows to get information about projection parts for tables of MergeTree family.
  */
class StorageSystemProjectionParts final : public StorageSystemPartsBase
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemProjectionParts> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemProjectionParts>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemProjectionParts(CreatePasskey, TArgs &&... args) : StorageSystemProjectionParts{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemProjectionParts"; }

protected:
    explicit StorageSystemProjectionParts(const StorageID & table_id_);
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};
}
