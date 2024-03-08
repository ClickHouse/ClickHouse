#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

struct Settings;

struct StatsSettings
{
    /// To calculate row count of aggregating result, when there is any column
    /// whose statistics is unknown, we use the coefficient to calculate the
    /// ndv of the first key.
    Float64 statistics_agg_unknown_column_first_key_coefficient;

    /// To calculate row count of aggregating result, we use the coefficient
    /// to calculate the ndv of the rest keys.
    Float64 statistics_agg_unknown_column_rest_key_coefficient;

    /// When calculating statistics fo preliminary aggregating, first we assume data is evenly
    /// distributed into shards and all shards has full cardinality of data set.
    /// But in practice a shard may have only partial of cardinality, so we
    /// multiply a coefficient.
    Float64 statistics_agg_full_cardinality_coefficient;

    static StatsSettings fromSettings(const Settings & from);
    static StatsSettings fromContext(ContextPtr from);
};

}
