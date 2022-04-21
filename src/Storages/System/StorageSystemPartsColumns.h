#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts_columns' which allows to get information about
 * columns in data parts for tables of MergeTree family.
 */
class StorageSystemPartsColumns final : public StorageSystemPartsBase
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemPartsColumns> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemPartsColumns>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemPartsColumns(CreatePasskey, TArgs &&... args) : StorageSystemPartsColumns{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemPartsColumns"; }

protected:
    explicit StorageSystemPartsColumns(const StorageID & table_id_);
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
