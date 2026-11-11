#pragma once

#include <optional>
#include "config.h"

#if USE_YTSAURUS

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage.h>
#include <Core/YTsaurus/YTsaurusClient.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>

namespace DB
{

struct StorageID;

struct YTsaurusStorageConfiguration
{
    YTsaurusSettings settings;
    std::vector<String> http_proxy_urls{};
    String cypress_path{};
    String oauth_token{};
    std::optional<String> ytsaurus_columns_description{};
};

/**
 *  Read only.
 *  One stream for dynamic table source.
 *  Multiple stream for static table source.
 */
class StorageYTsaurus final : public IStorage
{
public:
    static YTsaurusStorageConfiguration getConfiguration(ASTs engine_args, const YTsaurusSettings & settings, ContextPtr context, const StorageID * table_id = nullptr);

    static YTsaurusStorageConfiguration processNamedCollectionResult(const NamedCollection & named_collection, const YTsaurusSettings & setting, bool is_for_dictionary);


    StorageYTsaurus(
        const StorageID & table_id_,
        YTsaurusStorageConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "YTsaurus"; }
    bool isRemote() const override { return true; }
    bool isExternalDatabase() const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    const String cypress_path;
    YTsaurusSettings settings;
    YTsaurusClient::ConnectionInfo client_connection_info;

    LoggerPtr log;
};

}
#endif
