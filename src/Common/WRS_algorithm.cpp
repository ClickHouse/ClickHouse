#include <Common/WRS_algorithm.h>
#include <Common/thread_local_rng.h>

#include <cstddef>
#include <random>
#include <vector>

namespace DB
{

std::vector<size_t> pickWeightedRandom(std::span<const double> weights, size_t count)
{
    /// Can't pick more than initial set size
    count = std::min(count, weights.size());

    std::vector<std::pair<size_t, double>> indices_with_keys;
    indices_with_keys.reserve(indices_with_keys.size());

    std::uniform_real_distribution<double> dist(0, 1);
    for (size_t i = 0; i < weights.size(); ++i)
    {
        const double weight = weights[i];

        double u = dist(thread_local_rng);
        double key = std::pow(u, (1.0 / weight));

        indices_with_keys.emplace_back(i, key);
    }

    /// Sort in descending order to pickup top k
    std::partial_sort(indices_with_keys.begin(), indices_with_keys.begin() + count, indices_with_keys.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs.second > rhs.second;
    });

    std::vector<size_t> selected;
    for (size_t i = 0; i < count; ++i)
        selected.push_back(indices_with_keys[i].first);

    return selected;
}

}
