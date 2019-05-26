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
class StorageSystemOne : public ext::shared_ptr_helper<StorageSystemOne>, public IStorage
{
public:
    std::string getName() const override { return "SystemOne"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemOne(const std::string & name_);
};

}
