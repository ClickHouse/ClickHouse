#include <Interpreters/ClusterProxy/SelectQueryConstructor.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/MaterializingBlockInputStream.h>

namespace DB
{

namespace
{

constexpr PoolMode pool_mode = PoolMode::GET_MANY;

}

namespace ClusterProxy
{

SelectQueryConstructor::SelectQueryConstructor(
        QueryProcessingStage::Enum processed_stage_,
        QualifiedTableName main_table_,
        const Tables & external_tables_)
    : processed_stage{processed_stage_}
    , main_table(std::move(main_table_))
    , external_tables{external_tables_}
{
}

BlockInputStreamPtr SelectQueryConstructor::createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address)
{
    InterpreterSelectQuery interpreter{query_ast, context, processed_stage};
    BlockInputStreamPtr stream = interpreter.execute().in;

    /** Materialization is needed, since from remote servers the constants come materialized.
      * If you do not do this, different types (Const and non-Const) columns will be produced in different threads,
      * And this is not allowed, since all code is based on the assumption that in the block stream all types are the same.
      */
    return std::make_shared<MaterializingBlockInputStream>(stream);
}

BlockInputStreamPtr SelectQueryConstructor::createRemote(
        const ConnectionPoolWithFailoverPtr & pool, const std::string & query,
        const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
    auto stream = std::make_shared<RemoteBlockInputStream>(pool, query, &settings, throttler, external_tables, processed_stage, context);
    stream->setPoolMode(pool_mode);
    stream->setMainTable(main_table);
    return stream;
}

BlockInputStreamPtr SelectQueryConstructor::createRemote(
        ConnectionPoolWithFailoverPtrs && pools, const std::string & query,
        const Settings & settings, ThrottlerPtr throttler, const Context & context)
{
    auto stream = std::make_shared<RemoteBlockInputStream>(std::move(pools), query, &settings, throttler, external_tables, processed_stage, context);
    stream->setPoolMode(pool_mode);
    stream->setMainTable(main_table);
    return stream;
}

PoolMode SelectQueryConstructor::getPoolMode() const
{
    return pool_mode;
}

}

}
