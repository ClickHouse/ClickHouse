#include "../include/EntropyLeardnedHash.h"
#include <algorithm>
#include <cstring>
#include <map>
#include <set>

EntropyLearnedHashing::Key::Key(size_t size) : bytes_(std::vector<char>(size))
{
}

EntropyLearnedHashing::Key::Key(std::vector<char> bytes) : bytes_(std::move(bytes))
{
}

EntropyLearnedHashing::Key::Key(const char * ptr) : bytes_(ptr, ptr + strlen(ptr))
{
}

EntropyLearnedHashing::Key::Key(const char * ptr, const size_t len) : bytes_(ptr, ptr + len)
{
}

EntropyLearnedHashing::Key EntropyLearnedHashing::Key::GetSubKey(const std::vector<size_t> & positions) const
{
    std::vector<char> chosen_bytes;
    chosen_bytes.reserve(positions.size());
    for (auto position : positions)
    {
        if (position < bytes_.size())
        {
            chosen_bytes.push_back(bytes_[position]);
        }
    }
    return Key(chosen_bytes);
}

bool EntropyLearnedHashing::Key::operator<(const Key & other) const
{
    return bytes_ < other.bytes_;
}

bool EntropyLearnedHashing::Key::operator==(const Key & other) const
{
    return bytes_ == other.bytes_;
}

const std::vector<char> & EntropyLearnedHashing::Key::GetBytes() const
{
    return bytes_;
}

bool EntropyLearnedHashing::AreAllPartialKeysUnique(
    const std::vector<EntropyLearnedHashing::Key> & data, const std::vector<size_t> & positions)
{
    std::set<EntropyLearnedHashing::Key> partial_keys;
    for (const auto & key : data)
    {
        auto subkey = key.GetSubKey(positions);
        if (partial_keys.contains(subkey))
        {
            return false;
        }
        partial_keys.insert(subkey);
    }
    return true;
}

std::pair<size_t, uint64_t>
EntropyLearnedHashing::NextByte(const std::vector<EntropyLearnedHashing::Key> & data, size_t max_len, std::vector<size_t> bytes_chosen)
{
    uint64_t min_collisions = std::numeric_limits<uint64_t>::max();
    size_t best_position = 0;
    for (size_t i = 0; i < max_len; ++i)
    {
        bytes_chosen.push_back(i);
        std::map<EntropyLearnedHashing::Key, size_t> count_table;
        uint64_t collisions = 0;
        for (const auto & key : data)
        {
            auto partial_key = key.GetSubKey(bytes_chosen);
            collisions += count_table[partial_key];
            ++count_table[partial_key];
        }
        if (collisions < min_collisions)
        {
            min_collisions = collisions;
            best_position = i;
        }
        bytes_chosen.pop_back();
    }
    return {best_position, min_collisions};
}

std::pair<std::vector<size_t>, std::vector<uint64_t>>
EntropyLearnedHashing::ChooseBytes(const std::vector<EntropyLearnedHashing::Key> & train_data)
{
    // position will contain numbers of chosen bytes
    // entropies will contain entropies of keys after each new chosen byte
    // max_len will be a maximum length of any key in train_data
    std::vector<size_t> positions;
    std::vector<uint64_t> entropies;
    size_t max_len = 0;
    for (const auto & key : train_data)
    {
        max_len = std::max(max_len, key.GetBytes().size());
    }
    // while not all partial keys unique, choose new byte and recalculate an
    // entropy
    while (!EntropyLearnedHashing::AreAllPartialKeysUnique(train_data, positions))
    {
        auto [new_position, new_entropy] = EntropyLearnedHashing::NextByte(train_data, max_len, positions);
        if (new_entropy == (train_data.size()) * (train_data.size() - 1) / 2 || !entropies.empty() && new_entropy == entropies.back())
        {
            break;
        }
        positions.push_back(new_position);
        entropies.push_back(new_entropy);
    }
    return {positions, entropies};
}
