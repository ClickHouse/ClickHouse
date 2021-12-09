#pragma once

/// SparseHashMap is a wrapper for google::sparse_hash_map.

#include <sparsehash/sparse_hash_map>

template <class Key, class T, class HashFcn = std::hash<Key>,
    class EqualKey = std::equal_to<Key>,
    class Alloc = google::libc_allocator_with_realloc<std::pair<const Key, T>>>
using SparseHashMap = google::sparse_hash_map<Key, T, HashFcn, EqualKey, Alloc>;
