#include <Storages/NumberedFileName.h>

#include <algorithm>
#include <charconv>
#include <limits>
#include <optional>

namespace DB
{

namespace
{

struct SequenceNumberPosition
{
    /// The position of the dot before the number, or the position where the number has to be inserted.
    size_t begin;
    /// The position of the rest of the name - the extension.
    size_t end;
    /// The number, if the name already contains it.
    std::optional<size_t> number;
};

SequenceNumberPosition findSequenceNumber(const std::string & path)
{
    /// The name of the file ends at the first dot after the last slash.
    /// When there is no slash (a top-level object key), the whole string is the name: `npos + 1 == 0`.
    size_t begin = path.find_first_of('.', path.find_last_of('/') + 1);
    if (begin == std::string::npos)
        return {path.size(), path.size(), std::nullopt};

    /// The number, if it is there, is the next component of the name.
    size_t number_begin = begin + 1;
    size_t number_end = path.find('.', number_begin);
    if (number_end == std::string::npos)
        number_end = path.size();

    size_t number = 0;
    auto result = std::from_chars(path.data() + number_begin, path.data() + number_end, number);
    if (result.ec == std::errc{} && result.ptr == path.data() + number_end)
        return {begin, number_end, number};

    return {begin, begin, std::nullopt};
}

}

std::string setSequenceNumberInFileName(const std::string & path, size_t sequence_number)
{
    auto position = findSequenceNumber(path);
    return path.substr(0, position.begin) + "." + std::to_string(sequence_number) + path.substr(position.end);
}

size_t getStartSequenceNumber(const std::string & path, size_t default_number)
{
    auto number = findSequenceNumber(path).number;
    if (!number || *number == std::numeric_limits<size_t>::max())
        return default_number;
    return std::max(*number + 1, default_number);
}

}
