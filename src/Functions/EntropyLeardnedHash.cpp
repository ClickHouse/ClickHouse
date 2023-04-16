#include "../include/EntropyLeardnedHash.h"
#include <algorithm>
#include <cstring>
#include <map>
#include <set>

namespace
{
bool AreAllPartialKeysUnique(const std::vector<EntropyLearnedHashing::Key> & data, const std::vector<size_t> & positions)
{
    std::set<EntropyLearnedHashing::Key> partial_keys;
    for (const auto & key : data)
    {
        auto partial_key = key.getPartialKey(positions);
        if (partial_keys.contains(partial_key))
        {
            return false;
        }
        partial_keys.insert(partial_key);
    }
    return true;
}

// NextByte returns position of byte which adds the most entropy and the new
// entropy
std::pair<size_t, size_t> NextByte(const std::vector<EntropyLearnedHashing::Key> & data, size_t max_len, std::vector<size_t> past_bytes)
{
    size_t min_collisions = std::numeric_limits<size_t>::max();
    size_t best_position = 0;
    for (size_t i = 0; i < max_len; ++i)
    {
        past_bytes.push_back(i);
        std::map<EntropyLearnedHashing::Key, size_t> count_table;
        size_t collisions = 0;
        for (const auto & key : data)
        {
            auto partial_key = key.getPartialKey(past_bytes);
            collisions += count_table[partial_key];
            ++count_table[partial_key];
        }
        if (collisions < min_collisions)
        {
            min_collisions = collisions;
            best_position = i;
        }
        past_bytes.pop_back();
    }
    return {best_position, min_collisions};
}
}

EntropyLearnedHashing::Key::Key(size_t size) : bytes(std::vector<char>(size))
{
}

EntropyLearnedHashing::Key::Key(std::vector<char> bytes) : bytes(std::move(bytes))
{
}

EntropyLearnedHashing::Key::Key(std::vector<char> && bytes) : bytes(std::move(bytes))
{
}

EntropyLearnedHashing::Key::Key(const char * ptr) : bytes(ptr, ptr + strlen(ptr))
{
}

EntropyLearnedHashing::Key::Key(const char * ptr, size_t len) : bytes(ptr, ptr + len)
{
}

EntropyLearnedHashing::Key EntropyLearnedHashing::Key::getPartialKey(const std::vector<size_t> & positions) const
{
    std::vector<char> chosen_bytes;
    chosen_bytes.reserve(positions.size());
    for (auto position : positions)
        if (position < bytes.size())
            chosen_bytes.push_back(bytes[position]);
    return Key(std::move(chosen_bytes));
}

bool EntropyLearnedHashing::Key::operator<(const Key & other) const
{
    return bytes < other.bytes;
}

bool EntropyLearnedHashing::Key::operator==(const Key & other) const
{
    return bytes == other.bytes;
}

const std::vector<char> & EntropyLearnedHashing::Key::GetBytes() const
{
    return bytes;
}

std::pair<std::vector<size_t>, std::vector<size_t>>
EntropyLearnedHashing::ChooseBytes(const std::vector<EntropyLearnedHashing::Key> & train_data)
{
    const size_t train_data_size = train_data.size();
    // position will contain numbers of chosen bytes
    // entropies will contain entropies of keys after each new chosen byte
    // max_len will be a maximum length of any key in train_data
    std::vector<size_t> positions;
    std::vector<size_t> entropies;
    size_t max_len = 0;
    for (const auto & key : train_data)
        max_len = std::max(max_len, key.GetBytes().size());

    // while not all partial keys unique, choose new byte and recalculate an
    // entropy
    if (EntropyLearnedHashing::AreAllPartialKeysUnique(train_data, positions))
        return {};
    auto [new_position, new_entropy] = EntropyLearnedHashing::NextByte(train_data, max_len, positions);
    if ((new_entropy == train_data_size * (train_data_size - 1) / 2) {
        return {};
    }
    positions.push_back(new_position);
    entropies.push_back(new_entropy);

    while (!EntropyLearnedHashing::AreAllPartialKeysUnique(train_data, positions))
    {
        auto [new_position, new_entropy] = EntropyLearnedHashing::NextByte(train_data, max_len, positions);
        if (new_entropy == entropies.back())
            break;
        positions.push_back(new_position);
        entropies.push_back(new_entropy);
    }
    return {positions, entropies};
}
