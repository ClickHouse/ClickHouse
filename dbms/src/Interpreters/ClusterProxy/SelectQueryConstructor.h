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
    SelectQueryConstructor(const QueryProcessingStage::Enum & processed_stage, const Tables & external_tables);

    BlockInputStreamPtr createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address) override;
    BlockInputStreamPtr createRemote(ConnectionPoolPtr & pool, const std::string & query,
        const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
    BlockInputStreamPtr createRemote(ConnectionPoolsPtr & pools, const std::string & query,
        const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
    PoolMode getPoolMode() const override;

private:
    const QueryProcessingStage::Enum & processed_stage;
    const Tables & external_tables;
};

}

}
