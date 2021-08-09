#pragma once

#if defined(ARCADIA_BUILD)
#undef HASH_FUN_H
#define HASH_FUN_H <unordered_map>
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
#endif
