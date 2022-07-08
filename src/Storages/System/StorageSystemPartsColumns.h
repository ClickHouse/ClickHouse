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
public:
    explicit StorageSystemPartsColumns(const StorageID & table_id_);

    std::string getName() const override { return "SystemPartsColumns"; }

protected:
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
