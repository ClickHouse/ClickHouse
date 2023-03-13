#pragma once

#include <vector>
#include <optional>
#include <string_view>

namespace DB
{

class CharacterFinder
{
public:
    using Position = std::size_t;

    virtual ~CharacterFinder() = default;

    std::optional<Position> find_first(std::string_view haystack, const std::vector<char> & needles) const;

    virtual std::optional<Position> find_first(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const;

    std::optional<Position> find_first_not(std::string_view haystack, const std::vector<char> & needles) const;

    virtual std::optional<Position> find_first_not(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const;

};

/*
 * Maybe decorator would be better :)
 * */
class BoundsSafeCharacterFinder : CharacterFinder
{
public:
    std::optional<Position> find_first(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const override;

    std::optional<Position> find_first_not(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const override;
};

}

