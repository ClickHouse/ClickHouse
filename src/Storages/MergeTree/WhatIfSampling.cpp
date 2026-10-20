#include <Storages/MergeTree/WhatIfSampling.h>

#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>

#include <algorithm>
#include <cmath>

namespace DB
{

size_t samplingStep(UInt64 rows_to_scan, size_t units, UInt64 budget, UInt64 max_rows_to_read)
{
    if (max_rows_to_read != 0)
        budget = budget == 0 ? max_rows_to_read : std::min(budget, max_rows_to_read);
    if (budget == 0 || rows_to_scan <= budget)
        return 1;
    const size_t step = (rows_to_scan + budget - 1) / budget;
    /// the largest step that still samples `min_sampled_units`
    return std::min<size_t>(step, std::max<size_t>(1, (units - 1) / (min_sampled_units - 1)));
}

std::vector<size_t> sampleUnits(size_t units, size_t step, const String & part_name)
{
    std::vector<size_t> sampled;
    const UInt64 seed = sipHash64(part_name);
    for (size_t first = 0; first < units; first += step)
        sampled.push_back(first + intHash64(seed ^ first) % std::min(step, units - first));
    return sampled;
}

double standardErrorOfSample(const std::vector<double> & values)
{
    double sum_of_squares = 0;
    for (size_t i = 1; i < values.size(); ++i)
        sum_of_squares += (values[i] - values[i - 1]) * (values[i] - values[i - 1]);
    const double count = static_cast<double>(values.size());
    return std::sqrt(sum_of_squares / (2.0 * count * (count - 1.0)));
}

}
