#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemParts final : public ext::shared_ptr_helper<StorageSystemParts>, public StorageSystemPartsBase
{
    friend struct ext::shared_ptr_helper<StorageSystemParts>;
public:
    std::string getName() const override { return "SystemParts"; }

protected:
    explicit StorageSystemParts(const std::string & name_);
    void processNextStorage(MutableColumns & columns, const StoragesInfo & info, bool has_state_column) override;
};

}
