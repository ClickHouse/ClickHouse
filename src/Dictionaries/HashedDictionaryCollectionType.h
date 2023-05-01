#pragma once

#include <Dictionaries/IDictionary.h>
#include <Common/HashTable/PackedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <sparsehash/sparse_hash_map>
#include <sparsehash/sparse_hash_set>
#include <type_traits>

namespace DB
{

/// HashMap with packed structure is better than google::sparse_hash_map if the
/// <K, V> pair is small, for the sizeof(std::pair<K, V>) == 16, RSS for hash
/// table with 1e9 elements will be:
///
/// - google::sparse_hash_map             : 26GiB
/// - HashMap                             : 35GiB
/// - PackedHashMap                       : 22GiB
/// - google::sparse_hash_map<packed_pair>: 17GiB
///
/// Also note here sizeof(std::pair<>) was used since google::sparse_hash_map
/// uses it to store <K, V>, yes we can modify google::sparse_hash_map to work
/// with packed analog of std::pair, but the allocator overhead is still
/// significant, because of tons of reallocations (and those cannot be solved
/// with reserve() due to some internals of google::sparse_hash_map) and poor
/// jemalloc support of such pattern, which results in 33% fragmentation (in
/// comparison with glibc).
///
/// Plus since google::sparse_hash_map cannot use packed structure, it will
/// have the same memory footprint for everything from UInt8 to UInt64 values
/// and so on.
///
/// Returns true hen google::sparse_hash_map should be used, otherwise
/// PackedHashMap should be used instead.
template <typename K, typename V>
constexpr bool useSparseHashForHashedDictionary()
{
    return sizeof(PackedPairNoInit<K, V>) > 16;
}

///
/// Map (dictionary with attributes)
///

/// Type of the hash table for the dictionary.
template <DictionaryKeyType dictionary_key_type, bool sparse, typename Key, typename Value>
struct HashedDictionaryMapType;

/// Default implementation using builtin HashMap (for HASHED layout).
template <DictionaryKeyType dictionary_key_type, typename Key, typename Value>
struct HashedDictionaryMapType<dictionary_key_type, /* sparse= */ false, Key, Value>
{
    using Type = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashMap<UInt64, Value, DefaultHash<UInt64>>,
        HashMapWithSavedHash<StringRef, Value, DefaultHash<StringRef>>>;
};

/// Implementations for SPARSE_HASHED layout.
template <DictionaryKeyType dictionary_key_type, typename Key, typename Value, bool use_sparse_hash>
struct HashedDictionarySparseMapType;

/// Implementation based on google::sparse_hash_map for SPARSE_HASHED.
template <DictionaryKeyType dictionary_key_type, typename Key, typename Value>
struct HashedDictionarySparseMapType<dictionary_key_type, Key, Value, /* use_sparse_hash= */ true>
{
    /// Here we use sparse_hash_map with DefaultHash<> for the following reasons:
    ///
    /// - DefaultHash<> is used for HashMap
    /// - DefaultHash<> (from HashTable/Hash.h> works better then std::hash<>
    ///   in case of sequential set of keys, but with random access to this set, i.e.
    ///
    ///       SELECT number FROM numbers(3000000) ORDER BY rand()
    ///
    ///   And even though std::hash<> works better in some other cases,
    ///   DefaultHash<> is preferred since the difference for this particular
    ///   case is significant, i.e. it can be 10x+.
    using Type = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        google::sparse_hash_map<UInt64, Value, DefaultHash<Key>>,
        google::sparse_hash_map<StringRef, Value, DefaultHash<Key>>>;
};

/// Implementation based on PackedHashMap for SPARSE_HASHED.
template <DictionaryKeyType dictionary_key_type, typename Key, typename Value>
struct HashedDictionarySparseMapType<dictionary_key_type, Key, Value, /* use_sparse_hash= */ false>
{
    using Type = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        PackedHashMap<UInt64, Value, DefaultHash<UInt64>>,
        PackedHashMap<StringRef, Value, DefaultHash<StringRef>>>;
};
template <DictionaryKeyType dictionary_key_type, typename Key, typename Value>
struct HashedDictionaryMapType<dictionary_key_type, /* sparse= */ true, Key, Value>
    : public HashedDictionarySparseMapType<
        dictionary_key_type, Key, Value,
        /* use_sparse_hash= */ useSparseHashForHashedDictionary<Key, Value>()>
{};

///
/// Set (dictionary with attributes)
///

/// Type of the hash table for the dictionary.
template <DictionaryKeyType dictionary_key_type, bool sparse, typename Key>
struct HashedDictionarySetType;

/// Default implementation using builtin HashMap (for HASHED layout).
template <DictionaryKeyType dictionary_key_type, typename Key>
struct HashedDictionarySetType<dictionary_key_type, /* sparse= */ false, Key>
{
    using Type = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashSet<UInt64, DefaultHash<UInt64>>,
        HashSetWithSavedHash<StringRef, DefaultHash<StringRef>>>;
};

/// Implementation for SPARSE_HASHED.
///
/// NOTE: There is no implementation based on google::sparse_hash_set since
/// PackedHashMap is more optimal anyway (see comments for
/// useSparseHashForHashedDictionary()).
template <DictionaryKeyType dictionary_key_type, typename Key>
struct HashedDictionarySetType<dictionary_key_type, /* sparse= */ true, Key>
{
    using Type = std::conditional_t<
        dictionary_key_type == DictionaryKeyType::Simple,
        HashSet<UInt64, DefaultHash<UInt64>>,
        HashSet<StringRef, DefaultHash<StringRef>>>;
};

}
