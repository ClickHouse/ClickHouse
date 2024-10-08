#pragma once

#include <base/types.h>

namespace DB
{

/// Deprecated. Used only to define a type for the obsolete setting.
enum class ParallelReplicasCustomKeyFilterType : UInt8
{
    DEFAULT,
    RANGE,
};

enum class ParallelReplicasMode : UInt8
{
    AUTO = 0,
    READ_TASKS = 1,
    CUSTOM_KEY_SAMPLING = 2,
    CUSTOM_KEY_RANGE = 3,
    SAMPLING_KEY = 4,
};


}
