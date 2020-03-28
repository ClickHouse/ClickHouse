#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements storage for the system table One.
  * The table contains a single column of dummy UInt8 and a single row with a value of 0.
  * Used when the table is not specified in the query.
  * Analog of the DUAL table in Oracle and MySQL.
  */
class StorageSystemOne final : public StorageHelper<StorageSystemOne>, public IStorage
{
    friend struct StorageHelper<StorageSystemOne>;
public:
    std::string getName() const override { return "SystemOne"; }

    Pipes read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

protected:
    StorageSystemOne(const std::string & name_);
};

}
