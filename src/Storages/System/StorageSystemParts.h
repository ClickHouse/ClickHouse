#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemParts final : public shared_ptr_helper<StorageSystemParts>, public StorageSystemPartsBase
{
    friend struct shared_ptr_helper<StorageSystemParts>;
public:
    std::string getName() const override { return "SystemParts"; }

protected:
    explicit StorageSystemParts(const StorageID & table_id_);
    void processNextStorage(
        MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
