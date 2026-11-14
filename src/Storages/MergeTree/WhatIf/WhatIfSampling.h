#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{

/// the sampling of the `EXPLAIN WHATIF` scans, the same for the granules of a projection and the baseline marks of a skip index

/// fewer sampled units can't give an error estimate
constexpr size_t min_sampled_units = 30;

/// read one unit of every step: the rows over the budget, which `max_rows_to_read` lowers, but at least `min_sampled_units` units
size_t samplingStep(UInt64 rows_to_scan, size_t units, UInt64 budget, UInt64 max_rows_to_read);

/// one unit from every run of `step`, at a position hashed from the part name
/// so data that repeats with the step can't line up with the sample
std::vector<size_t> sampleUnits(size_t units, size_t step, const String & part_name);

/// the standard error of the mean of a systematic sample of at least two values, from the differences of successive values
/// with `weights` (one for each value) the mean is weighted, and the differences are of weight x (value - mean)
double standardErrorOfSample(const std::vector<double> & values, const std::vector<double> & weights = {});

}
