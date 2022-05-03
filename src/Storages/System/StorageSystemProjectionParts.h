#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'projection_parts' which allows to get information about projection parts for tables of MergeTree family.
  */
class StorageSystemProjectionParts final : public StorageSystemPartsBase
{
public:
    explicit StorageSystemProjectionParts(const StorageID & table_id_);

    std::string getName() const override { return "SystemProjectionParts"; }

protected:
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};
}
