#pragma once

#include <Parsers/IAST.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;
struct SelectQueryInfo;

class Pipe;
class QueryPlan;

namespace ClusterProxy
{

class IStreamFactory;

/// Update settings for Distributed query.
///
/// - Removes different restrictions (like max_concurrent_queries_for_user, max_memory_usage_for_user, etc.)
///   (but only if cluster does not have secret, since if it has, the user is the same)
/// - Update some settings depends on force_optimize_skip_unused_shards and:
///   - force_optimize_skip_unused_shards_nesting
///   - optimize_skip_unused_shards_nesting
///
/// @return new Context with adjusted settings
std::shared_ptr<Context> updateSettingsForCluster(const Cluster & cluster, const Context & context, const Settings & settings, Poco::Logger * log = nullptr);

/// Execute a distributed query, creating a vector of BlockInputStreams, from which the result can be read.
/// `stream_factory` object encapsulates the logic of creating streams for a different type of query
/// (currently SELECT, DESCRIBE).
void executeQuery(
    QueryPlan & query_plan,
    IStreamFactory & stream_factory, Poco::Logger * log,
    const ASTPtr & query_ast, const Context & context, const SelectQueryInfo & query_info);

}

}
