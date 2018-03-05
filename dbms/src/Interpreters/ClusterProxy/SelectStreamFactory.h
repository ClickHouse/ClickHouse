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
    SelectStreamFactory(
        const Block & header,
        QueryProcessingStage::Enum processed_stage,
        QualifiedTableName main_table,
        const Tables & external_tables);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res) override;

private:
    const Block header;
    QueryProcessingStage::Enum processed_stage;
    QualifiedTableName main_table;
    Tables external_tables;
};

}

}
