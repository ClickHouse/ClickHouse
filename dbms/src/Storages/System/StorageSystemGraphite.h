#pragma once

#include <Storages/IStorage.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{

/// Provides information about Graphite configuration.
class StorageSystemGraphite : public ext::shared_ptr_helper<StorageSystemGraphite>, public IStorage
{
public:
    std::string getName() const override { return "SystemGraphite"; }
    std::string getTableName() const override { return name; }

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:
    const std::string name;

protected:
    StorageSystemGraphite(const std::string & name_);
};

}
