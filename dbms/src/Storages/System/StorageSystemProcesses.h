#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `processes` system table, which allows you to get information about the queries that are currently executing.
  */
class StorageSystemProcesses : public ext::shared_ptr_helper<StorageSystemProcesses>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemProcesses>;
public:
    std::string getName() const override { return "SystemProcesses"; }
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

    StorageSystemProcesses(const std::string & name_);
};

}
