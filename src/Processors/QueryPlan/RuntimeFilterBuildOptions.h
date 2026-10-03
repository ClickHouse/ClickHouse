#pragma once

#include <optional>

#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Processors/QueryPlan/RuntimeFilterTypes.h>
#include <base/types.h>

namespace DB
{

struct RuntimeFilterBuildOptions
{
    UInt64 exact_values_limit;
    RuntimeBloomFilterParameters bloom;
    Float64 max_ratio_of_set_bits;
    RuntimeFilterPolarity polarity;
    RuntimeFilterMinMaxMode minmax_mode;
    bool track_key_range;
    std::optional<UInt64> distinct_keys_hint;
    bool distinct_keys_hint_matches_filter_key;
};

}
