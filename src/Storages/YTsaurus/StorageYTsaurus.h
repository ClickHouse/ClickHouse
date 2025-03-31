#pragma once

#include "config.h"

#if USE_YTSAURUS

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>


namespace DB
{

struct YTsaurusStorageConfiguration
{
    String http_proxy_url;
    String cypress_path;
    String oauth_token;
};

/**
 *  Read only.
 *  One stream only.
 */
class StorageYTsaurus final : public IStorage
{
public:
    static YTsaurusStorageConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

    StorageYTsaurus(
        const StorageID & table_id_,
        YTsaurusStorageConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "YTsaurus"; }
    bool isRemote() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    const YTsaurusStorageConfiguration configuration;
    LoggerPtr log;
};

}
#endif
