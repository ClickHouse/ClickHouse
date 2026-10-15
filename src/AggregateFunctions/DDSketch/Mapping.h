#pragma once

#include <cmath>
#include <limits>
#include <utility>
#include <base/types.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_DATA;
}

class DDSketchLogarithmicMapping
{
public:
    explicit DDSketchLogarithmicMapping(Float64 relative_accuracy_, Float64 offset_ = 0.0)
        : relative_accuracy(relative_accuracy_)
        , offset(offset_)
    {
        if (relative_accuracy <= 0 || relative_accuracy >= 1)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Relative accuracy must be between 0 and 1 but is {}", relative_accuracy);
        }

        gamma = (1 + relative_accuracy) / (1 - relative_accuracy);
        multiplier = 1 / std::log(gamma);
        updateBounds();
    }

    ~DDSketchLogarithmicMapping() = default;

    int key(Float64 value) const
    {
        if (value < min_possible || value > max_possible)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value {} is out of range [{}, {}]", value, min_possible, max_possible);
        }
        /// floor, not truncation: bin `k` is [γ^k, γ^(k+1)) for negative `k` too, else values below 1 land one bin
        /// too high, and `changeMapping` drops counts
        /// Values of the partial bin below `min_key` go one bin up: `validKeyRange` rejects that bin, and released
        /// versions check the same range, so they must be able to read the states we write. That bin is outside the
        /// relative accuracy guarantee
        return static_cast<int>(std::max(std::floor(logGamma(value) + offset), min_key));
    }

    Float64 value(int key) const { return lowerBound(key) * (1 + relative_accuracy); }

    /// The [min, max] bin keys that are valid to deserialize: the keys key() yields for the smallest
    /// and largest representable values, capped to what the store can index without overflowing
    /// Returns an empty range (every key rejected) when nothing is valid (a corrupted offset
    /// pushes every producible key outside the cap)
    std::pair<Int64, Int64> validKeyRange() const
    {
        static constexpr Int64 key_bound = Int64(1) << 30;
        const Float64 bound = static_cast<Float64>(key_bound);
        const Float64 min_value_key = logGamma(min_possible) + offset;
        const Float64 max_value_key = logGamma(max_possible) + offset;

        /// Empty value interval, no overlap with [-bound, bound], or NaN: nothing is valid.
        if (!(min_value_key <= max_value_key) || !(max_value_key >= -bound) || !(min_value_key <= bound))
            return {1, 0};

        return {min_value_key <= -bound ? -key_bound : static_cast<Int64>(min_value_key),
                max_value_key >= bound ? key_bound : static_cast<Int64>(max_value_key)};
    }

    Float64 logGamma(Float64 value) const { return std::log(value) * multiplier; }

    Float64 powGamma(Float64 value) const { return std::exp(value / multiplier); }

    Float64 lowerBound(int index) const { return powGamma(static_cast<Float64>(index) - offset); }

    Float64 getGamma() const { return gamma; }

    Float64 getMinPossible() const { return min_possible; }

    Float64 getMaxPossible() const { return max_possible; }

    bool operator==(const DDSketchLogarithmicMapping & other) const
    {
        if (gamma == other.gamma)
        {
            return true;
        }

        auto [min, max] = std::minmax(gamma, other.gamma);
        const Float64 difference = max - min;
        const Float64 acceptable = (std::nextafter(min, max) - min) * min;
        return difference <= acceptable;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(gamma, buf);
        writeBinary(offset, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(gamma, buf);
        readBinary(offset, buf);
        if (!std::isfinite(gamma) || gamma <= 1.0)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid gamma value after deserialization: {}", gamma);
        }
        if (!std::isfinite(offset))
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid offset value after deserialization: {}", offset);
        }
        /// ClickHouse only produces offset 0, and merge compares mappings by gamma alone, so it
        /// would merge differing offsets without remapping and corrupt the result
        if (offset != 0.0)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Nonzero DDSketch mapping offset is not supported: {}", offset);
        }
        multiplier = 1 / std::log(gamma);
        if (!std::isfinite(multiplier) || multiplier == 0.0)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid multiplier derived from gamma: {}", gamma);
        }
        /// serialized gamma defines the bins, so derive accuracy from it (inverse of the constructor)
        /// otherwise value() would mix the stored gamma with the accuracy from the aggregate type
        relative_accuracy = (gamma - 1) / (gamma + 1);
        /// relative accuracy outside (0,1) will lead to huge gamma which will break on merge
        if (!std::isfinite(relative_accuracy) || relative_accuracy <= 0.0 || relative_accuracy >= 1.0)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Invalid relative accuracy {} derived from gamma: {}",
                relative_accuracy,
                gamma);
        }
        updateBounds();
    }

protected:
    void updateBounds()
    {
        min_possible = std::numeric_limits<Float64>::min() * gamma;
        max_possible = std::numeric_limits<Float64>::max() / gamma;
        /// the same truncation as in `validKeyRange`
        min_key = std::trunc(logGamma(min_possible) + offset);
    }

    Float64 relative_accuracy;
    Float64 gamma;
    Float64 min_possible;
    Float64 max_possible;
    /// The lowest key `key` returns
    Float64 min_key;
    Float64 multiplier;
    Float64 offset;
};

}
