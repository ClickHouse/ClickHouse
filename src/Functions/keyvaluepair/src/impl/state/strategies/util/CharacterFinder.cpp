#include "CharacterFinder.h"
#include <base/find_symbols.h>


namespace DB
{

std::optional<CharacterFinder::Position> CharacterFinder::findFirst(std::string_view haystack, const std::vector<char> & needles)
{
    if (!needles.empty())
    {
        if (const auto * ptr = find_first_symbols_or_null(haystack, {needles.begin(), needles.end()}))
        {
            return ptr - haystack.begin();
        }
    }

    return std::nullopt;
}

std::optional<CharacterFinder::Position> CharacterFinder::findFirst(std::string_view haystack, std::size_t offset, const std::vector<char> & needles)
{
    if (auto position = findFirst({haystack.begin() + offset, haystack.end()}, needles))
    {
        return offset + *position;
    }

    return std::nullopt;
}

std::optional<CharacterFinder::Position> CharacterFinder::findFirstNot(std::string_view haystack, const std::vector<char> & needles)
{
    if (needles.empty())
    {
        return 0u;
    }

    if (const auto * ptr = find_first_not_symbols_or_null(haystack, {needles.begin(), needles.end()}))
    {
        return ptr - haystack.begin();
    }

    return std::nullopt;
}

std::optional<CharacterFinder::Position> CharacterFinder::findFirstNot(std::string_view haystack, std::size_t offset, const std::vector<char> & needles)
{
    if (auto position = findFirstNot({haystack.begin() + offset, haystack.end()}, needles))
    {
        return offset + *position;
    }

    return std::nullopt;
}

std::optional<BoundsSafeCharacterFinder::Position> BoundsSafeCharacterFinder::findFirst(
    std::string_view haystack, std::size_t offset, const std::vector<char> & needles
) const
{
    if (haystack.size() > offset)
    {
        return CharacterFinder::findFirst(haystack, offset, needles);
    }

    return std::nullopt;
}

std::optional<BoundsSafeCharacterFinder::Position> BoundsSafeCharacterFinder::findFirstNot(
    std::string_view haystack, std::size_t offset, const std::vector<char> & needles
) const
{
    if (haystack.size() > offset)
    {
        return CharacterFinder::findFirstNot(haystack, offset, needles);
    }

    return std::nullopt;
}

}
