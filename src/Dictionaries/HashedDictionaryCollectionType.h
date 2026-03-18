#pragma once

#include <Dictionaries/IDictionary.h>
#include <Common/HashTable/PackedHashMap.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Core/Types_fwd.h>
#include <sparsehash/sparse_hash_map>
#include <sparsehash/sparse_hash_set>
#include <type_traits>

namespace DB::HashedDictionaryImpl
{

/// Return true if the type is POD [1] for the purpose of layout (this is not
/// the same as STL traits has).
///
///   [1]: https://stackoverflow.com/questions/4178175/what-are-aggregates-and-pods-and-how-why-are-they-special/4178176#4178176
///
/// The behaviour had been change in clang-16, see this for more details:
/// - https://github.com/llvm/llvm-project/commit/a8b0c6fa28acced71db33e80bd0b51d00422035b
/// - https://github.com/llvm/llvm-project/commit/277123376ce08c98b07c154bf83e4092a5d4d3c6
/// - https://github.com/llvm/llvm-project/issues/62422
/// - https://github.com/llvm/llvm-project/issues/62353
/// - https://github.com/llvm/llvm-project/issues/62358
template <typename V>
constexpr bool isPodLayout()
{
    if constexpr (std::is_same_v<V, UUID>)
        return false;
    if constexpr (std::is_same_v<V, DateTime64>)
        return false;
    if constexpr (std::is_same_v<V, Decimal32> || std::is_same_v<V, Decimal64> || std::is_same_v<V, Decimal128> || std::is_same_v<V, Decimal256>)
        return false;
    if constexpr (std::is_same_v<V, StringRef>)
        return false;
    if constexpr (std::is_same_v<V, IPv6> || std::is_same_v<V, IPv4>)
        return false;
    return true;
}

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
    if constexpr (!isPodLayout<K>())
        return true;
    if constexpr (!isPodLayout<V>())
        return true;
    /// NOTE: One should not use PackedPairNoInit<K, V> here since this will
    /// create instantion of this type, and it could be illformed.
    return sizeof(V) > 8;
}

/// Grower with custom fill limit/load factor (instead of default 50%).
///
/// Based on HashTableGrowerWithPrecalculation
template <size_t initial_size_degree = 8>
class alignas(64) HashTableGrowerWithPrecalculationAndMaxLoadFactor
{
    UInt8 size_degree = initial_size_degree;
    size_t precalculated_mask = (1ULL << initial_size_degree) - 1;
    size_t precalculated_max_fill = 1ULL << (initial_size_degree - 1);
    float max_load_factor = 0.5;
    /// HashTableGrowerWithPrecalculation has 23, but to decrease memory usage
    /// at least slightly 19 is used here. Also note, that for dictionaries it
    /// is not that important since they are not that frequently loaded.
    static constexpr size_t max_size_degree_quadratic = 19;

public:
    static constexpr auto initial_count = 1ULL << initial_size_degree;

    /// If collision resolution chains are contiguous, we can implement erase operation by moving the elements.
    static constexpr auto performs_linear_probing_with_single_step = true;

    HashTableGrowerWithPrecalculationAndMaxLoadFactor() = default;
    explicit HashTableGrowerWithPrecalculationAndMaxLoadFactor(float max_load_factor_)
        : max_load_factor(max_load_factor_)
    {
        increaseSizeDegree(0);
    }

    UInt8 sizeDegree() const { return size_degree; }

    void increaseSizeDegree(UInt8 delta)
    {
        size_degree += delta;
        precalculated_mask = (1ULL << size_degree) - 1;
        precalculated_max_fill = static_cast<size_t>((1ULL << size_degree) * max_load_factor);
    }

    /// The size of the hash table in the cells.
    size_t bufSize() const { return 1ULL << size_degree; }

    /// From the hash value, get the cell number in the hash table.
    size_t place(size_t x) const { return x & precalculated_mask; }

    /// The next cell in the collision resolution chain.
    size_t next(size_t pos) const { return (pos + 1) & precalculated_mask; }

    /// Whether the hash table is sufficiently full. You need to increase the size of the hash table, or remove something unnecessary from it.
    bool overflow(size_t elems) const { return elems > precalculated_max_fill; }

    /// Increase the size of the hash table.
    void increaseSize() { increaseSizeDegree(size_degree >= max_size_degree_quadratic ? 1 : 2); }

    /// Set the buffer size by the number of elements in the hash table. Used when deserializing a hash table.
    void set(size_t num_elems)
    {
        if (num_elems <= 1)
            size_degree = initial_size_degree;
        else if (initial_size_degree > static_cast<size_t>(log2(num_elems - 1)) + 2)
            size_degree = initial_size_degree;
        else
        {
            /// Slightly more optimal than HashTableGrowerWithPrecalculation
            /// and takes into account max_load_factor.
            size_degree = static_cast<size_t>(log2(num_elems - 1)) + 1;
            if ((1ULL << size_degree) * max_load_factor < num_elems)
                ++size_degree;
        }
        increaseSizeDegree(0);
    }

    void setBufSize(size_t buf_size_)
    {
        size_degree = static_cast<size_t>(log2(buf_size_ - 1) + 1);
        increaseSizeDegree(0);
    }
};
static_assert(sizeof(HashTableGrowerWithPrecalculationAndMaxLoadFactor<>) == 64);

/// Above goes various specialisations for the hash table that will be used for
/// HASHED/SPARSE_HASHED dictionary, it could use one of the following depends
/// on the layout of the dictionary and types of key/value (for more info see
/// comments in this file):
/// - HashMap
/// - HashSet
/// - HashMapWithSavedHash
/// - HashSetWithSavedHash
/// - PackedHashMap
/// - google::sparse_hash_map

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
        HashMap<UInt64, Value, DefaultHash<UInt64>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>,
        HashMapWithSavedHash<StringRef, Value, DefaultHash<StringRef>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>>;
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
        PackedHashMap<UInt64, Value, DefaultHash<UInt64>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>,
        PackedHashMap<StringRef, Value, DefaultHash<StringRef>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>>;
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
        HashSet<UInt64, DefaultHash<UInt64>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>,
        HashSetWithSavedHash<StringRef, DefaultHash<StringRef>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>>;
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
        HashSet<UInt64, DefaultHash<UInt64>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>,
        HashSet<StringRef, DefaultHash<StringRef>, HashTableGrowerWithPrecalculationAndMaxLoadFactor<>>>;
};

}
