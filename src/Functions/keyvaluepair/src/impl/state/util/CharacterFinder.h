#pragma once

#include <vector>

namespace DB
{

class CharacterFinder
{
public:
    using Position = std::size_t;

    std::optional<Position> find_first(std::string_view haystack, const std::vector<char> & needles) const;

    std::optional<Position> find_first(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const;

    std::optional<Position> find_first_not(std::string_view haystack, const std::vector<char> & needles) const;

    std::optional<Position> find_first_not(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const;

};

}

