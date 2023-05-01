#pragma once

// Reference to the paper about entropy-learned hashing:
// https://dl.acm.org/doi/10.1145/3514221.3517894

#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>
#include "../../base/base/types.h"

// Key is a byte string
namespace EntropyLearnedHashing
{
using Key = String;

[[nodiscard]] Key getPartialKey(const std::string & key, const std::vector<size_t> & positions);

class IDsManager
{
public:
    static IDsManager & instance();

    // IDType is a type of an identifier of the dataset
    using IDType = std::string;
    // positions_by_id contains calculated best sets of positions to choose from keys for each id
    std::map<IDType, std::vector<size_t>> positions_by_id;
    static IDType default_id;
};

// ChooseBytes calls NextByte in loop
// until there is no pair of two different items of data with equal keys,
// or until there are no more useful bytes to choose.
// It returns a vector of chosen bytes and a vector entropies after each new byte addition.
// Entropy is the number of pairs of equal partial keys.
std::pair<std::vector<size_t>, std::vector<size_t>> ChooseBytes(const std::vector<Key> & train_data);
}
