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

}
