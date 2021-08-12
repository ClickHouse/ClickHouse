#pragma once
#include <unordered_map>

/// DenseHashMap is a wrapper for google::dense_hash_map.
/// Some hacks are needed to make it work in "Arcadia".
/// "Arcadia" is a proprietary monorepository in Yandex.
/// It uses slightly changed version of sparsehash with a different set of hash functions (which we don't need).
/// Those defines are needed to make it compile.
#if defined(ARCADIA_BUILD)
#define HASH_FUN_H <unordered_map>
template <typename T>
struct THash;
#endif

#include <sparsehash/dense_hash_map>

#if !defined(ARCADIA_BUILD)
    template <class Key, class T, class HashFcn = std::hash<Key>,
        class EqualKey = std::equal_to<Key>,
        class Alloc = google::libc_allocator_with_realloc<std::pair<const Key, T>>>
    using DenseHashMap = google::dense_hash_map<Key, T, HashFcn, EqualKey, Alloc>;
#else
    template <class Key, class T, class HashFcn = std::hash<Key>,
    class EqualKey = std::equal_to<Key>,
        class Alloc = google::sparsehash::libc_allocator_with_realloc<std::pair<const Key, T>>>
    using DenseHashMap = google::sparsehash::dense_hash_map<Key, T, HashFcn, EqualKey, Alloc>;

    #undef THash
#endif
