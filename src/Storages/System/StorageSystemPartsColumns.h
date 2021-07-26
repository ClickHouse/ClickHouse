#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'parts_columns' which allows to get information about
 * columns in data parts for tables of MergeTree family.
 */
class StorageSystemPartsColumns final
        : public ext::shared_ptr_helper<StorageSystemPartsColumns>, public StorageSystemPartsBase
{
    friend struct ext::shared_ptr_helper<StorageSystemPartsColumns>;
public:
    std::string getName() const override { return "SystemPartsColumns"; }

protected:
    StorageSystemPartsColumns(const StorageID & table_id_);
    void processNextStorage(
        MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
