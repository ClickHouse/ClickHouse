#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>
#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts_columns' which allows to get information about
 * columns in data parts for tables of MergeTree family.
 */
class StorageSystemPartsColumns
        : public ext::shared_ptr_helper<StorageSystemPartsColumns>, public StorageSystemPartsBase
{
public:
    std::string getName() const override { return "SystemPartsColumns"; }

protected:
    StorageSystemPartsColumns(const std::string & name_);
    void processNextStorage(MutableColumns & columns, const StoragesInfo & info, bool has_state_column) override;
};

}
