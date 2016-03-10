#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Storages/IStorage.h>
#include <DB/Client/ConnectionPool.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;

namespace ClusterProxy
{

class IQueryConstructor;

class Query
{
public:
	Query(IQueryConstructor & query_constructor_, const Cluster & cluster_,
		ASTPtr query_ast_, const Context & context_, const Settings & settings_, bool enable_shard_multiplexing_);
	BlockInputStreams execute();

private:
	IQueryConstructor & query_constructor;
	const Cluster & cluster;
	ASTPtr query_ast;
	const Context & context;
	const Settings & settings;
	bool enable_shard_multiplexing;
};

}

}
