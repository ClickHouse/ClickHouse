#pragma once

#include <base/types.h>

namespace DB
{

/// Whether a membership runtime filter keeps matching or non-matching values.
enum class RuntimeFilterPolarity : UInt8
{
    Contains,
    NotContains,
};

/// Controls whether an adaptive membership runtime filter also builds a numeric min/max filter.
enum class RuntimeFilterMinMaxMode : UInt8
{
    Disabled,
    Combined,
    Only,
};
}
