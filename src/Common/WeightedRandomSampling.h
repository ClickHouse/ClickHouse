#pragma once

#include <random>

#include <Common/thread_local_rng.h>

#include <base/defines.h>
#include <base/sort.h>


namespace DB
{

/// Weighted Random Sampling algorithm.
///
/// The implementation uses the A-ES method from the paper https://arxiv.org/pdf/1012.0256
///
/// @param data Container with objects to use for sampling.
/// @param get_weight Function that returns the weights of objects.
/// @param count Number of entries to sample.
/// @return Array of iterators pointing to the selected objects.
template <class Container, class GetWeightFunction>
std::vector<typename Container::const_iterator> pickWeightedRandom(const Container & data, GetWeightFunction get_weight, size_t count)
{
    using Iterator = Container::const_iterator;

    /// Can't pick more than initial set size
    count = std::min(count, data.size());

    std::vector<std::pair<Iterator, double>> keys;
    keys.reserve(keys.size());

    std::uniform_real_distribution<double> dist(0, 1);
    for (auto it = data.begin(); it != data.end(); ++it)
    {
        const double weight = get_weight(*it);
        chassert(weight > 0);

        double u = dist(thread_local_rng);
        double key = std::pow(u, (1.0 / weight));

        keys.emplace_back(it, key);
    }

    /// Sort in descending order to pickup top k
    ::partial_sort(
        keys.begin(), keys.begin() + count, keys.end(), [](const auto & lhs, const auto & rhs) { return lhs.second > rhs.second; });

    std::vector<Iterator> selected;
    selected.reserve(count);

    for (size_t i = 0; i < count; ++i)
        selected.push_back(keys[i].first);

    return selected;
}

}
