#pragma once

#include <Storages/System/StorageSystemPartsBase.h>


namespace DB
{

class Context;


/** Implements system table 'part_statistics' which exposes the column statistics
  * (minmax, uniq, tdigest, countmin, basic) stored in data parts of MergeTree tables.
  * One row per (part, column) pair for columns that have statistics in that part.
  */
class StorageSystemPartStatistics final : public StorageSystemPartsBase
{
public:
    explicit StorageSystemPartStatistics(const StorageID & table_id_);

    std::string getName() const override { return "SystemPartStatistics"; }

protected:
    void processNextStorage(
        ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) override;
};

}
