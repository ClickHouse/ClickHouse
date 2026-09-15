#pragma once

#include <cstdint>

namespace DB
{

/// Controls whether an adaptive membership runtime filter also builds a numeric min/max filter.
enum class RuntimeFilterMinMaxMode : uint8_t
{
    Disabled,
    Combined,
    Only,
};

}
