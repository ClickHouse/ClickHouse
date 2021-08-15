#pragma once

/// DenseHashSet is a wrapper for google::dense_hash_set.
/// See comment in DenseHashMap.h
#if defined(ARCADIA_BUILD)
#define HASH_FUN_H <unordered_map>
template <typename T>
struct THash;
#endif

#include <sparsehash/dense_hash_set>

#if !defined(ARCADIA_BUILD)
    template <class Value, class HashFcn = std::hash<Value>,
        class EqualKey = std::equal_to<Value>,
        class Alloc = google::libc_allocator_with_realloc<Value>>
    using DenseHashSet = google::dense_hash_set<Value, HashFcn, EqualKey, Alloc>;
#else
    template <class Value, class HashFcn = std::hash<Value>,
        class EqualKey = std::equal_to<Value>,
        class Alloc = google::sparsehash::libc_allocator_with_realloc<Value>>
    using DenseHashSet = google::sparsehash::dense_hash_set<Value, HashFcn, EqualKey, Alloc>;

    #undef THash
#endif
