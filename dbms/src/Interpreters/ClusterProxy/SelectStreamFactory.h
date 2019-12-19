#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
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
        QualifiedTableName main_table_,
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
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res) override;

private:
    const Block header;
    QueryProcessingStage::Enum processed_stage;
    QualifiedTableName main_table;
    ASTPtr table_func_ptr;
    Scalars scalars;
    bool has_virtual_shard_num_column = false;
    Tables external_tables;
};

}

}
