#pragma once

#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/IStorage.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>
#include <base/shared_ptr_helper.h>


namespace DB
{
class StorageMeiliSearch final : public shared_ptr_helper<StorageMeiliSearch>, public IStorage
{
    friend struct shared_ptr_helper<StorageMeiliSearch>;

public:
    StorageMeiliSearch(
        const StorageID & table_id,
        const MeiliSearchConfiguration & config_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "MeiliSearch"; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context) override;

    MeiliSearchConfiguration static getConfiguration(ASTs engine_args, ContextPtr context);

private:
    MeiliSearchConfiguration config;

    Poco::Logger * log;
};

}
