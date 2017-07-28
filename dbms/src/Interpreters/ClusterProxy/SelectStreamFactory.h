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

    BlockInputStreamPtr createLocal(const ASTPtr & query_ast, const Context & context, const Cluster::Address & address) override;
    BlockInputStreamPtr createRemote(
            const ConnectionPoolWithFailoverPtr & pool, const std::string & query,
            const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
    BlockInputStreamPtr createRemote(
            ConnectionPoolWithFailoverPtrs && pools, const std::string & query,
            const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
    PoolMode getPoolMode() const override;

private:
    QueryProcessingStage::Enum processed_stage;
    QualifiedTableName main_table;
    const Tables & external_tables;
};

}

}
