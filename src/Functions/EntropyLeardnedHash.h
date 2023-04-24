#pragma once

// Reference to the paper about entropy-learned hashing:
// https://bhentsch.github.io/doc/EntropyLearnedHashing.pdf

#include <cstdint>
#include <limits>
#include <string>
#include <vector>

// Key is a byte string
namespace EntropyLearnedHashing
{
using Key = std::string;

[[nodiscard]] Key getPartialKey(const std::string & key, const std::vector<size_t> & positions);

// ChooseBytes calls NextByte in loop
// until there is no pair of two different items of data with equal keys,
// or until there are no more useful bytes to choose.
// It returns a vector of chosen bytes and a vector entropies after each new byte addition.
// Entropy is the number of pairs of equal partial keys.
std::pair<std::vector<size_t>, std::vector<size_t>> ChooseBytes(const std::vector<Key> & train_data);
}
