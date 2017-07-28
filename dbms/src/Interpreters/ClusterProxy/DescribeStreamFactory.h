#pragma once

#include <Interpreters/ClusterProxy/IStreamFactory.h>

namespace DB
{

namespace ClusterProxy
{

class DescribeStreamFactory final : public IStreamFactory
{
public:
    DescribeStreamFactory() = default;

    BlockInputStreamPtr createLocal(const ASTPtr & query_ast, const Context & context, const Cluster::Address & address) override;
    BlockInputStreamPtr createRemote(
            const ConnectionPoolWithFailoverPtr & pool, const std::string & query,
            const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
    BlockInputStreamPtr createRemote(
            ConnectionPoolWithFailoverPtrs && pools, const std::string & query,
            const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
    PoolMode getPoolMode() const override;
};

}

}
