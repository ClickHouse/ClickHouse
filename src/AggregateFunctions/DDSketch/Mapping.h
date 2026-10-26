#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>
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
extern const int LOGICAL_ERROR;
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

    /// The buckets of an OpenTelemetry exponential histogram of `scale` (−5 to 18, not validated). Unlike the constructor,
    /// the gamma is exact
    static DDSketchLogarithmicMapping ofScale(Int64 scale)
    {
        DDSketchLogarithmicMapping result;
        result.setGamma(gammaOfScale(scale));
        return result;
    }

    /// 2^(2^-scale), the base of an OpenTelemetry exponential histogram. Not `exp2`, which rounds differently per libm: the
    /// gamma must have the same bits everywhere for states to merge without remapping
    static Float64 gammaOfScale(Int64 scale)
    {
        /// 2^1024 and above overflow, from scale -10
        if (scale <= -10)
            return std::numeric_limits<Float64>::infinity();
        if (scale <= 0)
            return std::ldexp(1.0, 1 << -scale);
        Float64 result = 2.0;
        for (Int64 i = 0; i < scale; ++i)
            result = std::sqrt(result);
        return result;
    }

    /// The scale whose `gammaOfScale` is `gamma_` (`sameGamma`), if there is one
    static std::optional<Int64> scaleOfGamma(Float64 gamma_)
    {
        const Int64 scale = static_cast<Int64>(-std::round(std::log2(std::log2(gamma_))));
        if (sameGamma(gammaOfScale(scale), gamma_))
            return scale;
        return {};
    }

    /// Whether two gammas give the same bins. They may differ by one ulp, because the same gamma computed another way can round differently
    static bool sameGamma(Float64 lhs, Float64 rhs) { return std::nextafter(lhs, rhs) == rhs; }

    /// The mapping with 2^shift times wider bins: e.g. for shift 2, its bin 1 is bins 4 to 7 of this one. Scale `s` becomes
    /// scale `s - shift`
    DDSketchLogarithmicMapping downscaled(int shift) const
    {
        DDSketchLogarithmicMapping result = *this;
        result.setGamma(downscaledGamma(shift));
        if (!std::isfinite(result.gamma) || !(result.relative_accuracy < 1.0)) [[unlikely]]
            throw Exception(ErrorCodes::LOGICAL_ERROR, "DDSketch gamma {} downscaled by {} is {}", gamma, shift, result.gamma);
        return result;
    }

    /// The `shift` for which `downscaled(shift) == coarser`, if there is one. If there is none and this mapping is of a scale, the
    /// `shift` to the finest scale that is not finer than `coarser`: e.g. from scale 6 to relative accuracy 0.01, the shift to scale 5
    std::optional<int> shiftTo(const DDSketchLogarithmicMapping & coarser) const
    {
        if (offset != coarser.offset)
            return {};
        const bool of_scale = scaleOfGamma(gamma).has_value();
        DDSketchLogarithmicMapping candidate = *this;
        /// Keys are within ±2³⁰ (`updateBounds`)
        for (int shift = 1; shift <= 30; ++shift)
        {
            /// Equals the gamma of `downscaled(shift)`, see `downscaledGamma`
            candidate.setGamma(candidate.downscaledGamma(1));
            /// A gamma that `downscaled` rejects, also infinity
            if (!(candidate.relative_accuracy < 1.0))
                return {};
            if (candidate == coarser || (of_scale && candidate.gamma > coarser.gamma))
                return shift;
            if (candidate.gamma > coarser.gamma)
                return {};
        }
        return {};
    }

    int key(Float64 value) const
    {
        if (value < min_possible || value > max_possible) [[unlikely]]
            throwValueOutOfRange(value);
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
    std::pair<Int64, Int64> validKeyRange() const { return {min_valid_key, max_valid_key}; }

    /// This mapping's key for `key` of a mapping with 2^shift times narrower bins
    Int64 downscaledKey(Int64 key, Int64 shift) const { return std::clamp(key >> shift, min_valid_key, max_valid_key); }

    Float64 logGamma(Float64 value) const { return std::log(value) * multiplier; }

    Float64 powGamma(Float64 value) const { return std::exp(value / multiplier); }

    Float64 lowerBound(int index) const { return powGamma(static_cast<Float64>(index) - offset); }

    Float64 getGamma() const { return gamma; }

    Float64 getMinPossible() const { return min_possible; }

    Float64 getMaxPossible() const { return max_possible; }

    bool operator==(const DDSketchLogarithmicMapping & other) const { return sameGamma(gamma, other.gamma); }

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
        /// serialized gamma defines the bins, so derive accuracy from it (inverse of the constructor)
        /// otherwise value() would mix the stored gamma with the accuracy from the aggregate type
        setGamma(gamma);
        if (!std::isfinite(multiplier) || multiplier == 0.0)
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid multiplier derived from gamma: {}", gamma);
        }
        /// relative accuracy outside (0,1) will lead to huge gamma which will break on merge
        if (!std::isfinite(relative_accuracy) || relative_accuracy <= 0.0 || relative_accuracy >= 1.0)
        {
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Invalid relative accuracy {} derived from gamma: {}",
                relative_accuracy,
                gamma);
        }
    }

protected:
    DDSketchLogarithmicMapping() = default;

    /// The gamma of `downscaled`, not validated. Squares, as a product is correctly rounded: the same on every platform, and by 1
    /// and then by 2 gives the same as by 3. With `exp(2^shift * log(gamma))`, states split differently would lose resolution on merge
    Float64 downscaledGamma(int shift) const
    {
        Float64 result = gamma;
        for (int i = 0; i < shift; ++i)
        {
            if (const auto scale = scaleOfGamma(result))
                return gammaOfScale(*scale - (shift - i));
            result *= result;
        }
        return result;
    }

    [[noreturn]] NO_INLINE void throwValueOutOfRange(Float64 value) const
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value {} is out of range [{}, {}]", value, min_possible, max_possible);
    }

    /// Sets `gamma` and everything derived from it; the callers validate the result
    void setGamma(Float64 gamma_)
    {
        gamma = gamma_;
        multiplier = 1 / std::log(gamma);
        relative_accuracy = (gamma - 1) / (gamma + 1);
        updateBounds();
    }

    void updateBounds()
    {
        min_possible = std::numeric_limits<Float64>::min() * gamma;
        max_possible = std::numeric_limits<Float64>::max() / gamma;
        const Float64 min_value_key = logGamma(min_possible) + offset;
        const Float64 max_value_key = logGamma(max_possible) + offset;
        /// the same truncation as for `min_valid_key` below
        min_key = std::trunc(min_value_key);

        static constexpr Int64 key_bound = Int64(1) << 30;
        const Float64 bound = static_cast<Float64>(key_bound);

        /// Empty value interval, no overlap with [-bound, bound], or NaN: nothing is valid.
        if (!(min_value_key <= max_value_key) || !(max_value_key >= -bound) || !(min_value_key <= bound))
        {
            min_valid_key = 1;
            max_valid_key = 0;
            return;
        }
        min_valid_key = min_value_key <= -bound ? -key_bound : static_cast<Int64>(min_value_key);
        max_valid_key = max_value_key >= bound ? key_bound : static_cast<Int64>(max_value_key);
    }

    Float64 relative_accuracy = 0.0;
    Float64 gamma = 0.0;
    Float64 min_possible = 0.0;
    Float64 max_possible = 0.0;
    /// The lowest key `key` returns
    Float64 min_key = 0.0;
    Int64 min_valid_key = 0;
    Int64 max_valid_key = 0;
    Float64 multiplier = 0.0;
    Float64 offset = 0.0;
};

}
