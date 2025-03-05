#pragma once

#include "config.h"

#if USE_YTSAURUS

#include <Interpreters/Context.h>
#include <Storages/IStorage.h>


namespace DB
{

struct YtsaurusStorageConfiguration
{
    String base_uri;
    String path;
    String auth_token;
};

/**
 *  Read only.
 *  One stream only.
 */
class StorageYtsaurus final : public IStorage
{
public:
    static YtsaurusStorageConfiguration getConfiguration(ASTs engine_args, ContextPtr context);

    StorageYtsaurus(
        const StorageID & table_id_,
        const YtsaurusStorageConfiguration configuration_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "Ytsaurus"; }
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
    const YtsaurusStorageConfiguration configuration;
    LoggerPtr log;
};

}
#endif
