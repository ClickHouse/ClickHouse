#pragma once

#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>

namespace DB
{

class StorageElasticsearch : public IStorage
{
public:

    static ElasticsearchConfiguration getConfiguration(ASTs & engine_args, ContextPtr context);

    StorageElasticsearch(
        const StorageID & table_id_,
        ElasticsearchConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constaints_,
        const String & comment_
    );

    std::string getName() const override { return "Elasticsearch"; }

    bool isRemote() const override { return true; }

    bool isExternalDatabase() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams
    ) override;

private:

    ElasticsearchConfiguration config;
    LoggerPtr log;
}

}
