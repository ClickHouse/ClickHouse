#pragma once

#include <base/types.h>
#include <optional>

namespace DB
{

class WriteBuffer;

namespace JSONBuilder
{
    class JSONMap;
}

struct JoinEstimation
{
    std::optional<UInt64> output_rows;
    std::optional<UInt64> left_rows;
    std::optional<UInt64> right_rows;
    std::optional<double> cost;
    std::optional<double> selectivity;
};

void describeJoinEstimation(const JoinEstimation & estimation, WriteBuffer & out, const String & prefix);
void describeJoinEstimation(const JoinEstimation & estimation, JSONBuilder::JSONMap & map);

}
