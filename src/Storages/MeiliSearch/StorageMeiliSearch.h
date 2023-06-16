#pragma once

#include <Storages/IStorage.h>
#include <Storages/MeiliSearch/MeiliSearchConnection.h>

namespace DB
{
class StorageMeiliSearch final : public IStorage
{
public:
    StorageMeiliSearch(
        const StorageID & table_id,
        const MeiliSearchConfiguration & config_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    String getName() const override { return "MeiliSearch"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert) override;

    static MeiliSearchConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

    static ColumnsDescription getTableStructureFromData(const MeiliSearchConfiguration & config_);

private:
    MeiliSearchConfiguration config;

    Poco::Logger * log;
};

}
