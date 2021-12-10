#pragma once

/// SparseHashMap is a wrapper for google::sparse_hash_map.

#include <sparsehash/sparse_hash_map>

/// Hash function for sparse hash table is very important, for example:
///
/// - DefaultHash<> (from HashTable/Hash.h> works better then std::hash<>
///   in case of sequential set of keys, but with random access to this set, i.e.
///
///       SELECT number FROM numbers(3000000) ORDER BY rand()
///
/// - but std::hash<> works good enough (and sometimes better) for generic cases
///
/// So std::hash<> by default is preferred.
template <class Key, class T, class HashFcn = std::hash<Key>,
    class EqualKey = std::equal_to<Key>,
    class Alloc = google::libc_allocator_with_realloc<std::pair<const Key, T>>>
using SparseHashMap = google::sparse_hash_map<Key, T, HashFcn, EqualKey, Alloc>;
