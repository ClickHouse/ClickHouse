#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/IStorage.h>


namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns : public ext::shared_ptr_helper<StorageSystemColumns>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemColumns>;
public:
    std::string getName() const override { return "SystemColumns"; }
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
    StorageSystemColumns(const std::string & name_);

private:
    const std::string name;
    NamesAndTypesList columns;
};

}
