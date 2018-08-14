#pragma once

#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/IStorage.h>

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
        const Tables & external_tables);

    /// TableFunction in a query.
    SelectStreamFactory(
        const Block & header_,
        QueryProcessingStage::Enum processed_stage_,
        ASTPtr table_func_ptr_,
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
    Tables external_tables;
};

}

}
