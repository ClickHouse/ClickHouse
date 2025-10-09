#pragma once

#include <base/types.h>
#include <cmath>
#include <stdexcept>
#include <limits>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class DDSketchLogarithmicMapping
{
public:
    explicit DDSketchLogarithmicMapping(Float64 relative_accuracy_, Float64 offset_ = 0.0)
        : relative_accuracy(relative_accuracy_), offset(offset_)
    {
        if (relative_accuracy <= 0 || relative_accuracy >= 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Relative accuracy must be between 0 and 1 but is {}", relative_accuracy);
        }

        gamma = (1 + relative_accuracy) / (1 - relative_accuracy);
        multiplier = 1 / std::log(gamma);
        min_possible = std::numeric_limits<Float64>::min() * gamma;
        max_possible = std::numeric_limits<Float64>::max() / gamma;
    }

    ~DDSketchLogarithmicMapping() = default;

    int key(Float64 value) const
    {
        if (value < min_possible || value > max_possible)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value {} is out of range [{}, {}]", value, min_possible, max_possible);
        }
        return static_cast<int>(logGamma(value) + offset);
    }

    Float64 value(int key) const
    {
        return lowerBound(key) * (1 + relative_accuracy);
    }

    Float64 logGamma(Float64 value) const
    {
        return std::log(value) * multiplier;
    }

    Float64 powGamma(Float64 value) const
    {
        return std::exp(value / multiplier);
    }

    Float64 lowerBound(int index) const
    {
        return powGamma(static_cast<Float64>(index) - offset);
    }

    Float64 getGamma() const
    {
        return gamma;
    }

    Float64 getMinPossible() const
    {
        return min_possible;
    }

    [[maybe_unused]] Float64 getMaxPossible() const
    {
        return max_possible;
    }

    void serialize(WriteBuffer& buf) const
    {
        writeBinary(gamma, buf);
        writeBinary(offset, buf);
    }

    void deserialize(ReadBuffer& buf)
    {
        readBinary(gamma, buf);
        readBinary(offset, buf);
        if (gamma <= 1.0)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid gamma value after deserialization: {}", gamma);
        }
        multiplier = 1 / std::log(gamma);
        min_possible = std::numeric_limits<Float64>::min() * gamma;
        max_possible = std::numeric_limits<Float64>::max() / gamma;
    }

protected:
    Float64 relative_accuracy;
    Float64 gamma;
    Float64 min_possible;
    Float64 max_possible;
    Float64 multiplier;
    Float64 offset;
};

}
