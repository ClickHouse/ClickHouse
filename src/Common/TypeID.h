#pragma once

#include <base/types.h>

#include <string_view>


namespace DB
{

/// Helpers for the TypeID format (https://github.com/jetify-com/typeid/tree/main/spec):
/// a type prefix plus a 26-character Crockford base32 suffix encoding a 128-bit UUID,
/// e.g. `user_01h455vb4pex5vsknk084sn02q`. When the prefix is empty, the `_` separator
/// is omitted; the empty string itself is not a valid TypeID.

constexpr size_t TYPE_ID_SUFFIX_LENGTH = 26;
constexpr size_t TYPE_ID_MAX_PREFIX_LENGTH = 63;

/// The prefix must match ^([a-z]([a-z_]{0,61}[a-z])?)?$: at most TYPE_ID_MAX_PREFIX_LENGTH
/// characters from [a-z_], starting and ending with [a-z]. The empty prefix is valid.
bool isValidTypeIDPrefix(std::string_view prefix);

/// Encodes the 128-bit UUID (given as the big-endian high and low halves) into
/// TYPE_ID_SUFFIX_LENGTH characters of lowercase Crockford base32. `dst` must have
/// room for TYPE_ID_SUFFIX_LENGTH bytes.
void encodeTypeIDSuffix(UInt64 high_bytes, UInt64 low_bytes, char * dst);

/// Decodes a TYPE_ID_SUFFIX_LENGTH-character suffix into the big-endian high and low
/// halves of the 128-bit UUID. Returns false if the suffix has a wrong length, contains
/// characters outside the alphabet, or overflows 128 bits (first character above '7').
bool decodeTypeIDSuffix(std::string_view suffix, UInt64 & high_bytes, UInt64 & low_bytes);

/// Splits a TypeID string into prefix and suffix at the last underscore and validates
/// the structure: the suffix length, the prefix contents, and the separator rules
/// (no separator for the empty prefix). The suffix characters are validated separately
/// by decodeTypeIDSuffix. Returns false if the string is malformed.
bool splitTypeID(std::string_view type_id, std::string_view & prefix, std::string_view & suffix);

}
