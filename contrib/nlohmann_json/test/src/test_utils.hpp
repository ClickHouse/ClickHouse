#pragma once

#include <cstdint> // uint8_t
#include <fstream> // ifstream, istreambuf_iterator, ios
#include <vector> // vector

namespace utils
{

inline std::vector<std::uint8_t> read_binary_file(const std::string& filename)
{
    std::ifstream file(filename, std::ios::binary);
    file.unsetf(std::ios::skipws);

    file.seekg(0, std::ios::end);
    const auto size = file.tellg();
    file.seekg(0, std::ios::beg);

    std::vector<std::uint8_t> byte_vector;
    byte_vector.reserve(static_cast<std::size_t>(size));
    byte_vector.insert(byte_vector.begin(), std::istream_iterator<std::uint8_t>(file), std::istream_iterator<std::uint8_t>());
    return byte_vector;
}

} // namespace utils
