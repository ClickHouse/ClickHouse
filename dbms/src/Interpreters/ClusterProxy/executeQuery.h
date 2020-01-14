#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>

namespace DB
{

struct Settings;
class Context;
class Cluster;

namespace ClusterProxy
{

/// removes different restrictions (like max_concurrent_queries_for_user, max_memory_usage_for_user, etc.)
/// from settings and creates new context with them
Context removeUserRestrictionsFromSettings(const Context & context, const Settings & settings);

}

}
