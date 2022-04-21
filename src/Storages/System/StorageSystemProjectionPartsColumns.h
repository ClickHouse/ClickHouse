#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'projection_parts_columns' which allows to get information about
 * columns in projection parts for tables of MergeTree family.
 */
class StorageSystemProjectionPartsColumns final : public StorageSystemPartsBase
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemProjectionPartsColumns> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemProjectionPartsColumns>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemProjectionPartsColumns(CreatePasskey, TArgs &&... args) : StorageSystemProjectionPartsColumns{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemProjectionPartsColumns"; }

protected:
    explicit StorageSystemProjectionPartsColumns(const StorageID & table_id_);
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};
}
