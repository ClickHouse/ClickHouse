#pragma once

#include <optional>

#include <Processors/QueryPlan/RuntimeFilterGeometry.h>
#include <Processors/QueryPlan/RuntimeFilterTypes.h>
#include <base/types.h>

namespace DB
{

struct RuntimeFilterBuildOptions
{
    RuntimeFilterGeometry geometry;
    RuntimeFilterPolarity polarity;
    bool track_key_range;
    std::optional<UInt64> distinct_keys_hint;
    bool distinct_keys_hint_matches_filter_key;
};

}
