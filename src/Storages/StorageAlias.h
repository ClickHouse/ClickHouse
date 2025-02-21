#pragma once

#include <Storages/IStorage.h>

namespace DB
{

/* An alias for another table. */
class StorageAlias final : public IStorage
{
public:
    StorageAlias(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_,
        const StorageID & ref_table_id_);
    std::string getName() const override { return "Alias"; }

    std::shared_ptr<IStorage> getRefStorage(ContextPtr context);
private:
    StorageID ref_table_id;
};

}
