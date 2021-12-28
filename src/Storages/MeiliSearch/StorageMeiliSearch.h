#pragma once

#include <base/shared_ptr_helper.h>

#include <Storages/IStorage.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>


namespace DB
{

class StorageMeiliSearch final : public shared_ptr_helper<StorageMeiliSearch>, public IStorage
{
    friend struct shared_ptr_helper<StorageMeiliSearch>;
public:
    StorageMeiliSearch(
        const StorageID & table_id,
        const MeiliSearchConfiguration& config_,
        const ColumnsDescription &  columns_,
        const ConstraintsDescription &  constraints_,
        const String &  comment);

    std::string getName() const override { return "MeiliSearch"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

private:

    MeiliSearchConfiguration getConfiguration(ASTs engine_args);

    MeiliSearchConfiguration config;

    Poco::Logger* log;

};

}
