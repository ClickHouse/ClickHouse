#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system.warnings table that contains warnings about server configuration
  * to be displayed in clickhouse-client.
  */
class StorageSystemWarnings final : public shared_ptr_helper<StorageSystemWarnings>,
                                public IStorageSystemOneBlock<StorageSystemWarnings> {
public:
    std::string getName() const override { return "SystemWarnings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemWarnings>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const override;
};
}
