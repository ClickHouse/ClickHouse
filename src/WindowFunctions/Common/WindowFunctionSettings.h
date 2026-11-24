#pragma once

#include <base/types.h>

namespace DB
{

struct Settings;

struct WindowFunctionSettings
{
    UInt64 sliding_aggregation_min_frame_rows = 0;

public:
    static WindowFunctionSettings create(const Settings & settings);
};

}
