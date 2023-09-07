#pragma once

#include <base/types.h>
#include <set>

namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{

String getEngineDefinitionFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, String default_partition_by = {}, String default_order_by = {}, const std::set<String> & engine_constraints = {}, const String & validation_query_description = {});

}
