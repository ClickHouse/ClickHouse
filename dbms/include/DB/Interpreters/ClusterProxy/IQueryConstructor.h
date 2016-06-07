#pragma once

#include <DB/Interpreters/Cluster.h>
#include <DB/Parsers/IAST.h>
#include <DB/Storages/IStorage.h>
#include <DB/Client/ConnectionPool.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;
class Throttler;

namespace ClusterProxy
{

/// Base class for the implementation of the details of distributed query
/// execution that are specific to the query type.
class IQueryConstructor
{
public:
	virtual ~IQueryConstructor() {}

	/// Create an input stream for local query execution.
	virtual BlockInputStreamPtr createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address) = 0;
	/// Create an input stream for remote query execution on one shard.
	virtual BlockInputStreamPtr createRemote(IConnectionPool * pool, const std::string & query,
		const Settings & settings, ThrottlerPtr throttler, const Context & context) = 0;
	/// Create an input stream for remote query execution on one or more shards.
	virtual BlockInputStreamPtr createRemote(ConnectionPoolsPtr & pools, const std::string & query,
		const Settings & new_settings, ThrottlerPtr throttler, const Context & context) = 0;
	/// Specify how we allocate connections on a shard.
	virtual PoolMode getPoolMode() const = 0;
};

}

}
