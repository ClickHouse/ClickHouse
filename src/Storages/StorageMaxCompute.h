#pragma once
#include "config.h"
#if USE_ODPS_TUNNEL
#include <Storages/IStorage.h>
namespace DB
{
class StorageMaxCompute : public IStorage
{
public:
    String getName() const override { return "MaxCompute"; }
    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;
    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override;
    size_t getMaxReadStreams(size_t num_streams, ContextPtr context) override;
public:
    StorageMaxCompute(
        const StorageID & table_id_,
        const std::string & odps_tunnel_endpoint_,
        const std::string & project_name_,
        const std::string & table_name_,
        const std::string & partition_spec_,
        const std::string & user_name_,
        const std::string & password_,
        const std::string & sts_token_,
        const uint64_t start_,
        const uint64_t count_,
        const uint64_t thread_num_,
        const std::string & odps_endpoint_,
        const std::string & quota_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const ASTPtr & settings_changes_);
private:
    std::string odps_tunnel_endpoint;
    std::string project_name;
    std::string table_name;
    std::string partition_spec;
    std::string user_name;
    std::string password;
    std::string sts_token;
    uint64_t start;
    uint64_t count;
    uint64_t thread_num;
    std::string odps_endpoint;
    std::string quota_name;
    Poco::Logger * log;
};
}
#endif
