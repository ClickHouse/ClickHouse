#pragma once

#include <vector>
#include <base/types.h>

namespace DB
{

/// Details about external machine learning model, used by clickhouse-server and clickhouse-library-bridge
struct ExternalModelInfo
{
    String model_path;
    String model_type;
    std::chrono::system_clock::time_point loading_start_time; /// serialized as std::time_t
    std::chrono::milliseconds loading_duration; /// serialized as UInt64
};

using ExternalModelInfos = std::vector<ExternalModelInfo>;

}
