#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'projection_parts' which allows to get information about projection parts for tables of MergeTree family.
  */
class StorageSystemProjectionParts final : public shared_ptr_helper<StorageSystemProjectionParts>, public StorageSystemPartsBase
{
    friend struct shared_ptr_helper<StorageSystemProjectionParts>;
public:
    std::string getName() const override { return "SystemProjectionParts"; }

protected:
    explicit StorageSystemProjectionParts(const StorageID & table_id_);
    void processNextStorage(
        MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};
}
