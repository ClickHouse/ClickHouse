#pragma once

#include <Client/ConnectionPool.h>
#include <Interpreters/Cluster.h>
#include <Parsers/IAST.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;
class Throttler;
struct SelectQueryInfo;

class Pipe;
using Pipes = std::vector<Pipe>;

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
            const SelectQueryInfo & query_info,
            Pipes & res) = 0;
};

}

}
