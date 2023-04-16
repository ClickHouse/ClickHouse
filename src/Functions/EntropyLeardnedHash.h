#pragma once

// Reference to the paper about entropy-learned hashing:
// https://bhentsch.github.io/doc/EntropyLearnedHashing.pdf

#include <cstdint>
#include <limits>
#include <vector>

// Key is a byte string
namespace EntropyLearnedHashing
{
class Key
{
public:
    Key() = default;

    explicit Key(size_t size);
    explicit Key(std::vector<char> bytes);
    explicit Key(std::vector<char> && bytes);
    explicit Key(const char * ptr);
    Key(const char * ptr, size_t len);

    // getPartialKey chooses stored in positions bytes from bytes_
    // positions, which are not less than bytes_.size(), are ignored
    [[nodiscard]] Key getPartialKey(const std::vector<size_t> & positions) const;

    bool operator<(const Key & other) const;
    bool operator==(const Key & other) const;

    [[nodiscard]] const std::vector<char> & GetBytes() const;

private:
    std::vector<char> bytes;
};

// ChooseBytes calls NextByte in loop until there is no pair of two different
// items of data with equal keys, or until there are no more useful bytes to
// choose. It returns a set of chosen bytes and entropies after each new byte
// addition. Entropy is the number of pair of equal partial keys.
std::pair<std::vector<size_t>, std::vector<size_t>> ChooseBytes(const std::vector<Key> & train_data);
}
