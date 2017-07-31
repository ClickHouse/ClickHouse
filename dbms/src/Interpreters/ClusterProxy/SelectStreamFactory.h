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
            QueryProcessingStage::Enum processed_stage,
            QualifiedTableName main_table,
            const Tables & external_tables);

    virtual void createForShard(
            const Cluster::ShardInfo & shard_info,
            const String & query, const ASTPtr & query_ast,
            const Context & context, const ThrottlerPtr & throttler,
            BlockInputStreams & res) override;

private:
    QueryProcessingStage::Enum processed_stage;
    QualifiedTableName main_table;
    const Tables & external_tables;
};

}

}
