#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Cluster.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;

namespace ClusterProxy
{

class IStreamFactory;

/// removes different restrictions (like max_concurrent_queries_for_user, max_memory_usage_for_user, etc.)
/// from settings and creates new context with them
Context removeUserRestrictionsFromSettings(const Context & context, const Settings & settings);

/// Execute a distributed query, creating a vector of BlockInputStreams, from which the result can be read.
/// `stream_factory` object encapsulates the logic of creating streams for a different type of query
/// (currently SELECT, DESCRIBE).
BlockInputStreams executeQuery(
    IStreamFactory & stream_factory, const ClusterPtr & cluster,
    const ASTPtr & query_ast, const Context & context, const Settings & settings);

}

}
