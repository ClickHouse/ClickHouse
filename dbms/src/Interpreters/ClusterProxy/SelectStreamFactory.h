#pragma once

#include <DataStreams/RemoteBlockInputStream.h>
#include <Core/QueryProcessingStage.h>
#include <Client/ConnectionPool.h>
#include <Interpreters/Cluster.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

namespace ClusterProxy
{

class SelectStreamFactory final
{
public:
    /// Database in a query.
    SelectStreamFactory(
        const Block & header_,
        QueryProcessingStage::Enum processed_stage_,
        QualifiedTableName main_table_,
        const Scalars & scalars_,
        bool has_virtual_shard_num_column_,
        const Tables & external_tables,
        const ClusterPtr & cluster,
        const ASTPtr & query_ast,
        const Context & context,
        const Settings & settings,
        const ThrottlerPtr & throttler);

    /// TableFunction in a query.
    SelectStreamFactory(
        const Block & header_,
        QueryProcessingStage::Enum processed_stage_,
        ASTPtr table_func_ptr_,
        const Scalars & scalars_,
        bool has_virtual_shard_num_column_,
        const Tables & external_tables_,
        const ClusterPtr & cluster,
        const ASTPtr & query_ast,
        const Context & context,
        const Settings & settings,
        const ThrottlerPtr & throttler);

    BlockInputStreams createStreams();

private:
    void createForShard(
        const Cluster::ShardInfo & shard_info,
        BlockInputStreams & result,
        RemoteBlockInputStream::ShardQueries & multiplexed_shards);

    const Block header;
    QueryProcessingStage::Enum processed_stage;
    QualifiedTableName main_table;
    ASTPtr table_func_ptr;
    Scalars scalars;
    bool has_virtual_shard_num_column = false;
    Tables external_tables;
    ClusterPtr cluster;
    ASTPtr query_ast;
    const Context & context;
    const Settings & settings;
    ThrottlerPtr throttler;
};

}

}
