#pragma once

#include <span>
#include <vector>

namespace DB
{

/// Weighted Random Sampling algorithm.
///
/// The implementation uses the A-ES method from the paper https://arxiv.org/pdf/1012.0256
///
/// @param weights Array that contains the weights of the elements from which the sampling will be performed.
/// @param count Number of entries to sample.
/// @return Indexes of selected objects.
std::vector<size_t> pickWeightedRandom(std::span<const double> weights, size_t count);

}
