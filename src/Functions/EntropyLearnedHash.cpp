#include "EntropyLearnedHash.h"
#include <algorithm>
#include <cstring>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace
{
bool AreAllPartialKeysUnique(const std::vector<EntropyLearnedHashing::Key> & data, const std::vector<size_t> & positions)
{
    std::unordered_set<EntropyLearnedHashing::Key> partial_keys;
    partial_keys.reserve(data.size());
    for (const auto & key : data)
    {
        if (!partial_keys.insert(EntropyLearnedHashing::getPartialKey(key, positions)).second)
            return false;
    }
    return true;
}

// NextByte returns position of byte which adds the most entropy and the new entropy
std::pair<size_t, size_t> NextByte(const std::vector<EntropyLearnedHashing::Key> & keys, size_t max_len, std::vector<size_t> & chosen_bytes)
{
    size_t min_collisions = std::numeric_limits<size_t>::max();
    size_t best_position = 0;

    std::unordered_map<EntropyLearnedHashing::Key, size_t> count_table;
    for (size_t i = 0; i < max_len; ++i)
    {
        count_table.clear();
        count_table.reserve(keys.size());

        chosen_bytes.push_back(i);
        size_t collisions = 0;
        for (const auto & key : keys)
        {
            auto partial_key = EntropyLearnedHashing::getPartialKey(key, chosen_bytes);
            collisions += count_table[partial_key]++;
        }

        if (collisions < min_collisions)
        {
            min_collisions = collisions;
            best_position = i;
        }
        chosen_bytes.pop_back();
    }
    return {best_position, min_collisions};
}

// std::pair<size_t, size_t> NextByte(const std::vector<EntropyLearnedHashing::Key> & keys, std::vector<size_t> & chosen_bytes)
// {
//     size_t max_len = 0;
//     for (const auto & key : keys)
//         max_len = std::max(max_len, key.size());

//     return NextByte(keys, max_len, chosen_bytes);
// }
}

namespace EntropyLearnedHashing
{
Key getPartialKey(const Key & key, const std::vector<size_t> & positions)
{
    Key result_key;
    result_key.reserve(positions.size());
    for (auto position : positions)
    {
        if (position < key.size())
            result_key.push_back(key[position]);
    }
    return result_key;
}

IDsManager & IDsManager::instance()
{
    static IDsManager instance;
    return instance;
}

std::pair<std::vector<size_t>, std::vector<size_t>> ChooseBytes(const std::vector<Key> & train_data)
{
    if (train_data.size() <= 1)
        return {};

    // position contains numbers of chosen bytes
    std::vector<size_t> positions;

    // entropies contains entropies of keys after each new chosen byte
    std::vector<size_t> entropies;

    // max_len is a maximal length of any key in train_data
    size_t max_len = 0;
    for (const auto & key : train_data)
        max_len = std::max(max_len, key.size());

    // while not all partial keys unique, choose new byte and recalculate the entropy
    while (!AreAllPartialKeysUnique(train_data, positions))
    {
        auto [new_position, new_entropy] = NextByte(train_data, max_len, positions);
        if (!entropies.empty() && new_entropy == entropies.back())
            break;
        positions.push_back(new_position);
        entropies.push_back(new_entropy);
    }
    return {positions, entropies};
}
}
