#pragma once

#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <tuple>
#include <type_traits>

/** This class provides a way to evaluate the error in the result of applying the HyperLogLog algorithm.
  * Empirical observations show that large errors occur at E < 5 * 2^precision, where
  * E is the return value of the HyperLogLog algorithm, and `precision` is the HyperLogLog precision parameter.
  * See "HyperLogLog in Practice: Algorithmic Engineering of a State of the Art Cardinality Estimation Algorithm".
  * (S. Heule et al., Proceedings of the EDBT 2013 Conference).
  */
template <typename BiasData>
class HyperLogLogBiasEstimator
{
public:
    static constexpr bool isTrivial()
    {
        return false;
    }

    /// Maximum number of unique values to which the correction should apply
    /// from the LinearCounting algorithm.
    static double getThreshold()
    {
        return BiasData::getThreshold();
    }

    /// Return the error estimate.
    static double getBias(double raw_estimate)
    {
        const auto & estimates = BiasData::getRawEstimates();
        const auto & biases = BiasData::getBiases();

        auto it = std::lower_bound(estimates.begin(), estimates.end(), raw_estimate);

        if (it == estimates.end())
        {
            return biases[estimates.size() - 1];
        }
        if (*it == raw_estimate)
        {
            size_t index = std::distance(estimates.begin(), it);
            return biases[index];
        }
        if (it == estimates.begin())
        {
            return biases[0];
        }

        /// We get the error estimate by linear interpolation.
        size_t index = std::distance(estimates.begin(), it);

        double estimate1 = estimates[index - 1];
        double estimate2 = estimates[index];

        double bias1 = biases[index - 1];
        double bias2 = biases[index];
        /// It is assumed that the estimate1 < estimate2 condition is always satisfied.
        double slope = (bias2 - bias1) / (estimate2 - estimate1);

        return bias1 + slope * (raw_estimate - estimate1);
    }

private:
    /// Static checks.
    using TRawEstimatesRef = decltype(BiasData::getRawEstimates());
    using TRawEstimates = std::remove_reference_t<TRawEstimatesRef>;

    using TBiasDataRef = decltype(BiasData::getBiases());
    using TBiasData = std::remove_reference_t<TBiasDataRef>;

    static_assert(std::is_same_v<TRawEstimates, TBiasData>, "Bias estimator data have inconsistent types");
    static_assert(std::tuple_size<TRawEstimates>::value > 0, "Bias estimator has no raw estimate data");
    static_assert(std::tuple_size<TBiasData>::value > 0, "Bias estimator has no bias data");
    static_assert(std::tuple_size<TRawEstimates>::value == std::tuple_size<TBiasData>::value,
                  "Bias estimator has inconsistent data");
};

/** Trivial case of HyperLogLogBiasEstimator: used if we do not want to fix
  * error. This has meaning for small values of the accuracy parameter, for example 5 or 12.
  * Then the corrections from the original version of the HyperLogLog algorithm are applied.
  * See "HyperLogLog: The analysis of a near-optimal cardinality estimation algorithm"
  * (P. Flajolet et al., AOFA '07: Proceedings of the 2007 International Conference on Analysis
  * of Algorithms)
  */
struct TrivialBiasEstimator
{
    static constexpr bool isTrivial()
    {
        return true;
    }

    static double getThreshold()
    {
        return 0.0;
    }

    static double getBias(double /*raw_estimate*/)
    {
        return 0.0;
    }
};
