#pragma once

#include <Interpreters/Context_fwd.h>
#include <Core/QueryProcessingStage.h>
#include <Parsers/IAST.h>

namespace DB
{

struct Settings;
class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;
struct SelectQueryInfo;

class Pipe;
class QueryPlan;

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

struct StorageID;

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
ContextMutablePtr updateSettingsForCluster(
    const Cluster & cluster, ContextPtr context, const Settings & settings, Poco::Logger * log = nullptr);

/// Execute a distributed query, creating a vector of BlockInputStreams, from which the result can be read.
/// `stream_factory` object encapsulates the logic of creating streams for a different type of query
/// (currently SELECT, DESCRIBE).
void executeQuery(
    QueryPlan & query_plan,
    const Block & header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    IStreamFactory & stream_factory, Poco::Logger * log,
    const ASTPtr & query_ast, ContextPtr context, const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const ClusterPtr & not_optimized_cluster);

}

}
