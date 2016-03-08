#pragma once

#include <DB/Interpreters/Cluster.h>
#include <DB/Interpreters/ClusterProxy/PreSendHook.h>
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

class IQueryConstructor
{
public:
	virtual ~IQueryConstructor() {}

	void setPreSendHook(PreSendHook & pre_send_hook_) { pre_send_hook = pre_send_hook_; }
	void setupBarrier(size_t count) { if (pre_send_hook) { pre_send_hook.setupBarrier(count); } }

	virtual BlockInputStreamPtr createLocal(ASTPtr query_ast, const Context & context, const Cluster::Address & address) = 0;
	virtual BlockInputStreamPtr createRemote(IConnectionPool * pool, const std::string & query,
		const Settings & settings, ThrottlerPtr throttler, const Context & context) = 0;
	virtual BlockInputStreamPtr createRemote(ConnectionPoolsPtr & pools, const std::string & query,
		const Settings & new_settings, ThrottlerPtr throttler, const Context & context) = 0;
	virtual bool isInclusive() const = 0;

protected:
	PreSendHook pre_send_hook;
};

}

}
