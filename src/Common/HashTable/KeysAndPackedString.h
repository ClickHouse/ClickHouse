#pragma once

#include <type_traits>

#include <base/PackedStringRef.h>

/// The key of a `GROUP BY` over fixed-width columns and one `String` column (the `keys32_packed_string`,
/// `keys64_packed_string` and `keys128_packed_string` aggregation methods): the fixed-width values packed into `Keys`
/// (`UInt32`, `UInt64` or `UInt128`, laid out as in the `keys32`, `keys64` and `keys128` methods) and the string as a
/// packed reference. After 4 bytes of `Keys`, a `PackedStringRef` would leave 4 bytes of padding, so the string is a
/// `PackedStringRef20` there: the key takes 24 bytes either way, and strings of up to 15 bytes stay inline instead of
/// 11.
///
/// The 32-bit hash stored in the packed reference covers the whole key, the fixed-width part included, so it
/// is the hash of the key (see `HashMethodKeysAndPackedString`). There are two exceptions. An empty string
/// stores no hash: such keys are hashed by their fixed-width part alone (see `KeysAndPackedStringHashMap`).
/// A `PackedStringRef` of a string over 4 GiB stores the length of the string in place of the hash (see
/// `PackedStringRef::build`), so such keys of one length share a hash. Equality compares whole keys, so these
/// keys stay correct, and a table can hold only a few keys that large, so the shared hash costs nothing.
template <typename Keys>
struct KeysAndPackedString
{
    using PackedString = std::conditional_t<sizeof(Keys) <= 4, PackedStringRef20, PackedStringRef>;

    Keys keys;
    PackedString str;

    /// The string goes first: it starts with the stored hash, so one comparison rejects almost any other key.
    ALWAYS_INLINE bool operator==(const KeysAndPackedString & rhs) const { return str == rhs.str && keys == rhs.keys; }
};

template <typename T>
constexpr bool is_keys_and_packed_string = false;

template <typename Keys>
constexpr bool is_keys_and_packed_string<KeysAndPackedString<Keys>> = true;
