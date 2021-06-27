#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

namespace ClusterProxy
{

class SelectStreamFactory final : public IStreamFactory
{
public:
    /// Database in a query.
    SelectStreamFactory(
        const Block & header_,
        QueryProcessingStage::Enum processed_stage_,
        StorageID main_table_,
        const Scalars & scalars_,
        bool has_virtual_shard_num_column_,
        const Tables & external_tables);

    /// TableFunction in a query.
    SelectStreamFactory(
        const Block & header_,
        QueryProcessingStage::Enum processed_stage_,
        ASTPtr table_func_ptr_,
        const Scalars & scalars_,
        bool has_virtual_shard_num_column_,
        const Tables & external_tables_);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        ContextPtr context, const ThrottlerPtr & throttler,
        const SelectQueryInfo & query_info,
        std::vector<QueryPlanPtr> & plans,
        Pipes & remote_pipes,
        Pipes & delayed_pipes,
        Poco::Logger * log) override;

private:
    const Block header;
    QueryProcessingStage::Enum processed_stage;
    StorageID main_table = StorageID::createEmpty();
    ASTPtr table_func_ptr;
    Scalars scalars;
    bool has_virtual_shard_num_column = false;
    Tables external_tables;
};

}

}
