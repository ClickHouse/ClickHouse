#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements system table 'dropped_tables_parts' which allows to get information about data parts for dropped but not yet removed tables.
  */
class StorageSystemDroppedTablesParts final : public IStorageSystemOneBlock<StorageSystemDroppedTablesParts>
{
public:
    std::string getName() const override { return "SystemDroppedTablesParts"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
