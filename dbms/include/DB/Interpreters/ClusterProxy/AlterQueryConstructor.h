#pragma once

#include <DB/Interpreters/ClusterProxy/IQueryConstructor.h>

namespace DB
{

namespace ClusterProxy
{

class AlterQueryConstructor final : public IQueryConstructor
{
public:
	AlterQueryConstructor() = default;

	BlockInputStreamPtr createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address) override;
	BlockInputStreamPtr createRemote(ConnectionPoolPtr & pool, const std::string & query,
		const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
	BlockInputStreamPtr createRemote(ConnectionPoolsPtr & pools, const std::string & query,
		const Settings & settings, ThrottlerPtr throttler, const Context & context) override;
	PoolMode getPoolMode() const override;
};

}

}
