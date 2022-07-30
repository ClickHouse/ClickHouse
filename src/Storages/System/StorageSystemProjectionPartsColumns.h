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
public:
    explicit StorageSystemProjectionPartsColumns(const StorageID & table_id_);

    std::string getName() const override { return "SystemProjectionPartsColumns"; }

protected:
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};
}
