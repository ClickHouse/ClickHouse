#pragma once

#include <base/types.h>
#include <string>
#include <variant>
#include <vector>

namespace DB
{

struct StepMetric
{
    std::string name;
    std::variant<Int64, UInt64, double, std::string> value;

    enum class Format { Raw, Bytes, Quantity, Time, Percent };
    Format format = Format::Raw;
};

using StepAnalyzeInfo = std::vector<StepMetric>;

}
