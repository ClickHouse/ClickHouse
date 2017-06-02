#pragma once

#include <ext/shared_ptr_helper.hpp>
#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `replicas` system table, which provides information about the status of the replicated tables.
  */
class StorageSystemReplicas : private ext::shared_ptr_helper<StorageSystemReplicas>, public IStorage
{
friend class ext::shared_ptr_helper<StorageSystemReplicas>;

public:
    static StoragePtr create(const std::string & name_);

    std::string getName() const override { return "SystemReplicas"; }
    std::string getTableName() const override { return name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return columns; }

    BlockInputStreams read(
        const Names & column_names,
        const ASTPtr & query,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;
    NamesAndTypesList columns;

    StorageSystemReplicas(const std::string & name_);
};

}
