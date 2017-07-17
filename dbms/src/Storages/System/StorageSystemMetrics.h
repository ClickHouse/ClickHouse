#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `metrics` system table, which provides information about the operation of the server.
  */
class StorageSystemMetrics : public ext::shared_ptr_helper<StorageSystemMetrics>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemMetrics>;
public:
    std::string getName() const override { return "SystemMetrics"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    NamesAndTypesList columns;

    StorageSystemMetrics(const std::string & name_);
};

}
