#pragma once

// Reference to the paper about entropy-learned hashing:
// https://bhentsch.github.io/doc/EntropyLearnedHashing.pdf

#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <vector>

// Key is a byte string
namespace EntropyLearnedHashing
{
using Key = std::string;

[[nodiscard]] Key getPartialKey(const std::string & key, const std::vector<size_t> & positions);

class IDsManager
{
private:
    static IDsManager * instance_ptr;
    IDsManager() { }
    IDsManager(const IDsManager &);
    IDsManager & operator=(IDsManager &);

public:
    static IDsManager * getInstance()
    {
        if (!IDsManager::instance_ptr)
            IDsManager::instance_ptr = new IDsManager();
        return IDsManager::instance_ptr;
    }

    using IDType = std::string;
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
