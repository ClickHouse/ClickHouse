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

class IStreamFactory;

/// Execute a distributed query, creating a vector of BlockInputStreams, from which the result can be read.
/// If `enable_shard_multiplexing` is false, each stream corresponds to a single shard.
/// `stream_factory` object encapsulates the logic of creating streams for a different type of query
/// (currently SELECT, DESCRIBE, or ALTER (for resharding)).
BlockInputStreams executeQuery(
        IStreamFactory & stream_factory, const ClusterPtr & cluster,
        const ASTPtr & query_ast, const Context & context, const Settings & settings, bool enable_shard_multiplexing);

}

}
