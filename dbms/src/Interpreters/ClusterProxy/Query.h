#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;

namespace ClusterProxy
{

class IQueryConstructor;

/// This class is designed for distributed queries execution. It hides from
/// the caller the details about the actual locations at which a distributed
/// query is performed. Depending on the type of query to be performed,
/// (currently SELECT, DESCRIBE, or ALTER (for resharding)), a so-called
/// query constructor is specified. Such an object states, among other things,
/// how connections must be allocated for remote execution.
class Query
{
public:
    Query(IQueryConstructor & query_constructor_, const ClusterPtr & cluster_,
        const ASTPtr & query_ast_, const Context & context_, const Settings & settings_, bool enable_shard_multiplexing_);

    /// For each location at which we perform the query, create an input stream
    /// from which we can fetch the result.
    BlockInputStreams execute();

private:
    IQueryConstructor & query_constructor;
    ClusterPtr cluster;
    ASTPtr query_ast;
    const Context & context;
    const Settings & settings;
    bool enable_shard_multiplexing;
};

}

}
