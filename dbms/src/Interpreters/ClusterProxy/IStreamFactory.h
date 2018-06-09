#pragma once

#include <Interpreters/Cluster.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Client/ConnectionPool.h>

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
class IStreamFactory
{
public:
    virtual ~IStreamFactory() {}

    virtual void createForShard(
            const Cluster::ShardInfo & shard_info,
            const String & query, const ASTPtr & query_ast,
            const Context & context, const ThrottlerPtr & throttler,
            BlockInputStreams & res) = 0;
};

}

}
