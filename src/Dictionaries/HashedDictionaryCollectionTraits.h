#pragma once

#include <type_traits>
#include <sparsehash/sparse_hash_map>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/PackedHashMap.h>

namespace DB::HashedDictionaryImpl
{

/// sparse_hash_map/sparse_hash_set
template <typename C>
concept IsGoogleSparseHashTable = std::is_same_v<C, google::sparse_hash_map<
    typename C::key_type,
    typename C::mapped_type,
    /// HashFcn is not exported in sparse_hash_map is public type
    DefaultHash<typename C::key_type>>>;

template <typename V>
concept IsStdMapCell = requires (V v)
{
    v->first;
    v->second;
};

/// HashMap/HashMapWithSavedHash/HashSet/HashMapWithSavedHash/PackedHashMap and their Cells
template <typename C>
concept IsBuiltinHashTable = (
    std::is_same_v<C, HashMapWithSavedHash<
        typename C::key_type,
        typename C::mapped_type,
        DefaultHash<typename C::key_type>,
        typename C::grower_type>> ||
    std::is_same_v<C, HashMap<
        typename C::key_type,
        typename C::mapped_type,
        DefaultHash<typename C::key_type>,
        typename C::grower_type>> ||
    std::is_same_v<C, PackedHashMap<
        typename C::key_type,
        typename C::mapped_type,
        DefaultHash<typename C::key_type>,
        typename C::grower_type>> ||
    std::is_same_v<C, HashSetWithSavedHash<
        typename C::key_type,
        DefaultHash<typename C::key_type>,
        typename C::grower_type>> ||
    std::is_same_v<C, HashSet<
        typename C::key_type,
        DefaultHash<typename C::key_type>,
        typename C::grower_type>>
);

template <typename V>
concept IsBuiltinSetCell = requires (V v)
{
    v.getKey();
};

template <typename V>
concept IsBuiltinMapCell = requires (V v)
{
    v->getKey();
    v->getMapped();
};

// NOLINTBEGIN(*)

/// google::sparse_hash_map
template <typename T> auto getSetKeyFromCell(const T & value) { return value; }
template <typename T> auto getKeyFromCell(const T & value) requires (IsStdMapCell<T>) { return value->first; }
template <typename T> auto getValueFromCell(const T & value) requires (IsStdMapCell<T>) { return value->second; }

/// size() - returns table size, without empty and deleted
/// and since this is sparsehash, empty cells should not be significant,
/// and since items cannot be removed from the dictionary, deleted is also not important.
///
/// NOTE: for google::sparse_hash_set value_type is Key, for sparse_hash_map
/// value_type is std::pair<Key, Value>, and now we correctly takes into
/// account padding in structures, if any.
template <typename C> auto getBufferSizeInBytes(const C & c) requires (IsGoogleSparseHashTable<C>) { return c.size() * sizeof(typename C::value_type); }
/// bucket_count() - Returns table size, that includes empty and deleted
template <typename C> auto getBufferSizeInCells(const C & c) requires (IsGoogleSparseHashTable<C>) { return c.bucket_count(); }

template <typename C> auto resizeContainer(C & c, size_t size) requires (IsGoogleSparseHashTable<C>) { return c.resize(size); }
template <typename C> auto clearContainer(C & c) requires (IsGoogleSparseHashTable<C>) { return c.clear(); }

/// HashMap
template <typename T> auto getSetKeyFromCell(const T & value) requires (IsBuiltinSetCell<T>) { return value.getKey(); }
template <typename T> auto getKeyFromCell(const T & value) requires (IsBuiltinMapCell<T>) { return value->getKey(); }
template <typename T> auto getValueFromCell(const T & value) requires (IsBuiltinMapCell<T>) { return value->getMapped(); }

template <typename C> auto getBufferSizeInBytes(const C & c) requires (IsBuiltinHashTable<C>) { return c.getBufferSizeInBytes(); }
template <typename C> auto getBufferSizeInCells(const C & c) requires (IsBuiltinHashTable<C>) { return c.getBufferSizeInCells(); }
template <typename C> auto resizeContainer(C & c, size_t size) requires (IsBuiltinHashTable<C>) { return c.reserve(size); }
template <typename C> void clearContainer(C & c) requires (IsBuiltinHashTable<C>) { return c.clearAndShrink(); }

// NOLINTEND(*)

}
