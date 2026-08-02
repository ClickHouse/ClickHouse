#include <Common/TypeID.h>

namespace DB
{

namespace
{

constexpr char type_id_alphabet[] = "0123456789abcdefghjkmnpqrstvwxyz";

/// Inverse of type_id_alphabet: character -> 5-bit value, -1 for characters outside the
/// alphabet. Note that Crockford's ambiguous characters (i, l, o, u) and uppercase letters
/// are invalid: the TypeID spec requires the exact lowercase alphabet with no aliases.
// clang-format off
constexpr Int8 map_digits[256] =
{
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
     0,  1,  2,  3,  4,  5,  6,  7,  8,  9, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, 10, 11, 12, 13, 14, 15, 16, 17, -1, 18, 19, -1, 20, 21, -1,
    22, 23, 24, 25, 26, -1, 27, 28, 29, 30, 31, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};
// clang-format on

}

bool isValidTypeIDPrefix(std::string_view prefix)
{
    if (prefix.empty())
        return true;

    if (prefix.size() > TYPE_ID_MAX_PREFIX_LENGTH)
        return false;

    if (prefix.front() == '_' || prefix.back() == '_')
        return false;

    for (char c : prefix)
        if (!((c >= 'a' && c <= 'z') || c == '_'))
            return false;

    return true;
}

void encodeTypeIDSuffix(UInt64 high_bytes, UInt64 low_bytes, char * dst)
{
    /// The 128-bit value is prepended with two zero bits and split into 26 groups
    /// of 5 bits, most significant first, so the first character is always '0'-'7'.
    using UInt128Raw = unsigned __int128;
    UInt128Raw value = (static_cast<UInt128Raw>(high_bytes) << 64) | low_bytes;

    for (size_t i = 0; i < TYPE_ID_SUFFIX_LENGTH; ++i)
        dst[i] = type_id_alphabet[static_cast<UInt8>(value >> (125 - 5 * i)) & 0x1F];
}

bool decodeTypeIDSuffix(std::string_view suffix, UInt64 & high_bytes, UInt64 & low_bytes)
{
    if (suffix.size() != TYPE_ID_SUFFIX_LENGTH)
        return false;

    /// The first character carries the two padding bits, so anything above '7'
    /// would overflow 128 bits.
    if (suffix[0] < '0' || suffix[0] > '7')
        return false;

    using UInt128Raw = unsigned __int128;
    UInt128Raw value = 0;

    for (char c : suffix)
    {
        Int8 digit = map_digits[static_cast<UInt8>(c)];
        if (digit == -1)
            return false;
        value = (value << 5) | static_cast<UInt8>(digit);
    }

    high_bytes = static_cast<UInt64>(value >> 64);
    low_bytes = static_cast<UInt64>(value);
    return true;
}

bool splitTypeID(std::string_view type_id, std::string_view & prefix, std::string_view & suffix)
{
    /// The empty string is not a valid TypeID.
    if (type_id.empty())
        return false;

    /// The suffix never contains an underscore, so the separator is the last one.
    size_t separator_pos = type_id.rfind('_');
    if (separator_pos == std::string_view::npos)
    {
        prefix = {};
        suffix = type_id;
    }
    else
    {
        /// A separator with the empty prefix is invalid: the nil-prefix form has no separator.
        if (separator_pos == 0)
            return false;
        prefix = type_id.substr(0, separator_pos);
        suffix = type_id.substr(separator_pos + 1);
    }

    if (suffix.size() != TYPE_ID_SUFFIX_LENGTH)
        return false;

    return isValidTypeIDPrefix(prefix);
}

}
