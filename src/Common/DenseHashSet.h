#pragma once

#if defined(ARCADIA_BUILD)
#undef HASH_FUN_H
#define HASH_FUN_H <unordered_map>
#endif

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmacro-redefined"
#endif
#include <sparsehash/dense_hash_set>
#ifdef __clang__
#pragma clang diagnostic pop
#endif

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
