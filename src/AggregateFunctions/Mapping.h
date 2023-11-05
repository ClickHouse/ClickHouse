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

class KeyMapping
{
public:
    explicit KeyMapping(Float64 relative_accuracy_, Float64 offset_ = 0.0)
        : relative_accuracy(relative_accuracy_), offset(offset_)
    {

        if (relative_accuracy <= 0 || relative_accuracy >= 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Relative accuracy must be between 0 and 1.");
        }

        Float64 gamma_mantissa = 2 * relative_accuracy / (1 - relative_accuracy);
        gamma = 1 + gamma_mantissa;
        multiplier = 1 / std::log1p(gamma_mantissa);
        min_possible = std::numeric_limits<Float64>::min() * gamma;
        max_possible = std::numeric_limits<Float64>::max() / gamma;
    }

    virtual ~KeyMapping() = default;  // Virtual destructor

    virtual Float64 logGamma(Float64 value) const = 0;
    virtual Float64 powGamma(Float64 value) const = 0;

    int key(Float64 value) const
    {
        return static_cast<int>(std::ceil(logGamma(value)) + offset);
    }

    Float64 value(int key) const
    {
        return powGamma(key - offset) * (2.0 / (1 + gamma));
    }

    Float64 getGamma() const
    {
        return gamma;
    }

    Float64 getMinPossible() const
    {
        return min_possible;
    }

    Float64 getMaxPossible() const
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
    }

protected:
    Float64 relative_accuracy;
    Float64 gamma;
    Float64 min_possible;
    Float64 max_possible;
    Float64 multiplier;
    Float64 offset;
};

class LogarithmicMapping : public KeyMapping
{
public:
    explicit LogarithmicMapping(Float64 relative_accuracy_, Float64 offset_ = 0.0)
        : KeyMapping(relative_accuracy_, offset_)
    {
        multiplier *= std::log(2);
    }

    ~LogarithmicMapping() override = default;  // Virtual destructor

    Float64 logGamma(Float64 value) const override
    {
        return std::log(value) / std::log(2.0) * multiplier;
    }

    Float64 powGamma(Float64 value) const override
    {
        return std::pow(2.0, value / multiplier);
    }
};

}
