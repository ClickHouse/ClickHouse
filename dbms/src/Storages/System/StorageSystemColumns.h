#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns : public ext::shared_ptr_helper<StorageSystemColumns>, public IStorage
{
public:
    std::string getName() const override { return "SystemColumns"; }

    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        UInt64 max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemColumns(const std::string & name_);
};

}
