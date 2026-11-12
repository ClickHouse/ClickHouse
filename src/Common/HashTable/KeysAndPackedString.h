#pragma once

#include <base/PackedStringRef.h>
#include <base/types.h>

/// The key of a `GROUP BY` over fixed-width columns and one `String` column (the `keys32_packed_string`,
/// `keys64_packed_string` and `keys128_packed_string` aggregation methods): the fixed-width values packed
/// into `Keys` (`UInt32`, `UInt64` or `UInt128`, laid out as in the `keys32`, `keys64` and `keys128` methods)
/// and the string as a packed reference.
///
/// The key stores a 32-bit hash that covers the whole key, the fixed-width part included, so it is the hash
/// of the key (`getHash`, see `HashMethodKeysAndPackedString`). The `PackedStringRef` of the `UInt64` and
/// `UInt128` keys holds it in its first 4 bytes. There are two exceptions. An empty string stores no hash:
/// such keys are hashed by their fixed-width part alone (see `KeysAndPackedStringHashTable`). A
/// `PackedStringRef` of a string over 4 GiB stores the length of the string in place of the hash (see
/// `PackedStringRef::build`), so such keys of one length share a hash. Equality compares whole keys, so these
/// keys stay correct, and a table can hold only a few keys that large, so the shared hash costs nothing.
template <typename Keys>
struct KeysAndPackedString
{
    using PackedString = PackedStringRef;

    Keys keys;
    PackedString str;

    /// Builds the key of the packed values `keys` and the string of `size` bytes at `data`. A non-empty
    /// string stores `hash_fn(data, size)`, a 32-bit hash, as the hash of the key. The caller guarantees 15
    /// readable bytes past the string: the most that the packed string of any key width reads (see
    /// `PackedString::build`).
    template <typename HashFn>
    static ALWAYS_INLINE KeysAndPackedString build(Keys keys, const char * data, size_t size, HashFn && hash_fn)
    {
        return {keys, PackedString::build(data, size, hash_fn)};
    }

    /// The key of the packed values `keys` and an empty string.
    static ALWAYS_INLINE KeysAndPackedString withEmptyString(Keys keys) { return {keys, {}}; }

    /// The hash that the key stores (see above): 0 for an empty string, whose key a table hashes by `keys`.
    ALWAYS_INLINE UInt32 getHash() const { return str.getHash(); }

    /// The string goes first: it starts with the stored hash, so one comparison rejects almost any other key.
    ALWAYS_INLINE bool operator==(const KeysAndPackedString & rhs) const { return str == rhs.str && keys == rhs.keys; }
};

/// After the 4 bytes of a `UInt32`, a `PackedStringRef` would leave 4 bytes of padding. So the string is an
/// `UnhashedPackedStringRef`, which is 4-byte aligned and has no hash, and the hash takes those 4 bytes. The
/// key takes 24 bytes either way, and strings of up to 15 bytes stay inline instead of 11.
template <>
struct KeysAndPackedString<UInt32>
{
    using PackedString = UnhashedPackedStringRef;

    UInt32 keys;
    /// The hash of the key, 0 for an empty string.
    UInt32 hash;
    PackedString str;

    /// Builds the key as the primary template does, with the same contract.
    template <typename HashFn>
    static ALWAYS_INLINE KeysAndPackedString build(UInt32 keys, const char * data, size_t size, HashFn && hash_fn)
    {
        if (size == 0)
            return withEmptyString(keys);
        return {keys, hash_fn(data, size), PackedString::build(data, size)};
    }

    static ALWAYS_INLINE KeysAndPackedString withEmptyString(UInt32 keys) { return {keys, 0, {}}; }

    ALWAYS_INLINE UInt32 getHash() const { return hash; }

    /// The hash goes first, so one comparison rejects almost any other key.
    ALWAYS_INLINE bool operator==(const KeysAndPackedString & rhs) const
    {
        return hash == rhs.hash && str == rhs.str && keys == rhs.keys;
    }
};

static_assert(sizeof(KeysAndPackedString<UInt32>) == 24 && alignof(KeysAndPackedString<UInt32>) == 4);

template <typename T>
constexpr bool is_keys_and_packed_string = false;

template <typename Keys>
constexpr bool is_keys_and_packed_string<KeysAndPackedString<Keys>> = true;
