#pragma once

#include <Columns/IColumn_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/Elasticsearch/ElasticsearchSettings.h>
#include <Storages/IStorage.h>
#include <Common/logger_useful.h>
#include <Storages/Elasticsearch/ElasticsearchConfiguration.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/VirtualColumnsDescription.h>

namespace DB
{

class StorageElasticsearch : public IStorage
{
public:

    StorageElasticsearch(
        const StorageID & table_id_,
        ElasticsearchConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment_,
        std::unique_ptr<ElasticsearchSettings> settings_
    );

    std::string getName() const override { return "Elasticsearch"; }

    bool isRemote() const override { return true; }

    bool isExternalDatabase() const override { return true; }

    bool supportsColumnsWithDynamicStructure() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams
    ) override;

    const ElasticsearchSettings & getSettings() const { return *storage_settings; }

    static ColumnsDescription getDefaultColumns();

    static void checkColumns(const ColumnsDescription & descriptions);
private:

    VirtualColumnsDescription createVirtuals();
    std::unique_ptr<ElasticsearchSettings> storage_settings;
    ElasticsearchConfiguration config;
};

}
