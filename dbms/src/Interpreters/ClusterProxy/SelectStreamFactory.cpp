#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>

namespace DB
{

namespace ClusterProxy
{

SelectStreamFactory::SelectStreamFactory(
        QueryProcessingStage::Enum processed_stage_,
        QualifiedTableName main_table_,
        const Tables & external_tables_)
    : processed_stage{processed_stage_}
    , main_table(std::move(main_table_))
    , external_tables{external_tables_}
{
}

void SelectStreamFactory::createForShard(
        const Cluster::ShardInfo & shard_info,
        const String & query, const ASTPtr & query_ast,
        const Context & context, const ThrottlerPtr & throttler,
        BlockInputStreams & res)
{
    if (shard_info.isLocal())
    {
        InterpreterSelectQuery interpreter{query_ast, context, processed_stage};
        BlockInputStreamPtr stream = interpreter.execute().in;

        /** Materialization is needed, since from remote servers the constants come materialized.
         * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
         * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
         */
        res.emplace_back(std::make_shared<MaterializingBlockInputStream>(stream));
    }
    else
    {
        auto stream = std::make_shared<RemoteBlockInputStream>(shard_info.pool, query, &context.getSettingsRef(), context, throttler, external_tables, processed_stage);
        stream->setPoolMode(PoolMode::GET_MANY);
        stream->setMainTable(main_table);
        res.emplace_back(std::move(stream));
    }
}

}
}
