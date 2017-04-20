#pragma once

#include <Interpreters/ClusterProxy/IQueryConstructor.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ClusterProxy
{

class SelectQueryConstructor final : public IQueryConstructor
{
public:
    SelectQueryConstructor(
            QueryProcessingStage::Enum processed_stage,
            QualifiedTableName main_table,
            const Tables & external_tables);

    BlockInputStreamPtr createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address) override;
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
