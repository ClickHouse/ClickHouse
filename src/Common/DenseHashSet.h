#pragma once

#if defined(ARCADIA_BUILD)
#undef HASH_FUN_H
#define HASH_FUN_H <unordered_map>
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
#endif
