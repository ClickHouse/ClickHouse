#pragma once

/// SparseHashMap is a wrapper for google::sparse_hash_map.
#if defined(ARCADIA_BUILD)
#define HASH_FUN_H <unordered_map>
template <typename T>
struct THash;
#endif

#include <sparsehash/sparse_hash_map>

#if !defined(ARCADIA_BUILD)
    template <class Key, class T, class HashFcn = std::hash<Key>,
        class EqualKey = std::equal_to<Key>,
        class Alloc = google::libc_allocator_with_realloc<std::pair<const Key, T>>>
    using SparseHashMap = google::sparse_hash_map<Key, T, HashFcn, EqualKey, Alloc>;
#else
    template <class Key, class T, class HashFcn = std::hash<Key>,
        class EqualKey = std::equal_to<Key>,
        class Alloc = google::sparsehash::libc_allocator_with_realloc<std::pair<const Key, T>>>
    using SparseHashMap = google::sparsehash::sparse_hash_map<Key, T, HashFcn, EqualKey, Alloc>;

    #undef THash
#endif
