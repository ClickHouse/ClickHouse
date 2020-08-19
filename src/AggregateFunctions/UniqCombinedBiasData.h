#pragma once

#include <array>

namespace DB
{

/** Data for HyperLogLogBiasEstimator in the uniqCombined function.
  * The development plan is as follows:
  * 1. Assemble ClickHouse.
  * 2. Run the script src/src/scripts/gen-bias-data.py, which returns one array for getRawEstimates()
  *     and another array for getBiases().
  * 3. Update `raw_estimates` and `biases` arrays. Also update the size of arrays in InterpolatedData.
  * 4. Assemble ClickHouse.
  * 5. Run the script src/src/scripts/linear-counting-threshold.py, which creates 3 files:
  * - raw_graph.txt (1st column: the present number of unique values;
  *    2nd column: relative error in the case of HyperLogLog without applying any corrections)
  * - linear_counting_graph.txt (1st column: the present number of unique values;
  *    2nd column: relative error in the case of HyperLogLog using LinearCounting)
  * - bias_corrected_graph.txt (1st column: the present number of unique values;
  *    2nd column: relative error in the case of HyperLogLog with the use of corrections from the algorithm HyperLogLog++)
  * 6. Generate a graph with gnuplot based on this data.
  * 7. Determine the minimum number of unique values at which it is better to correct the error
  *     using its evaluation (ie, using the HyperLogLog++ algorithm) than applying the LinearCounting algorithm.
  * 7. Accordingly, update the constant in the function getThreshold()
  * 8. Assemble ClickHouse.
  */
struct UniqCombinedBiasData
{
    using InterpolatedData = std::array<double, 200>;

    static double getThreshold();
    /// Estimates of the number of unique values using the HyperLogLog algorithm without applying any corrections.
    static const InterpolatedData & getRawEstimates();
    /// Corresponding error estimates.
    static const InterpolatedData & getBiases();
};

}
