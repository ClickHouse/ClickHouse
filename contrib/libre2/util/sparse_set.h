// Copyright 2006 The RE2 Authors.  All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#ifndef UTIL_SPARSE_SET_H_
#define UTIL_SPARSE_SET_H_

// DESCRIPTION
//
// SparseSet(m) is a set of integers in [0, m).
// It requires sizeof(int)*m memory, but it provides
// fast iteration through the elements in the set and fast clearing
// of the set.
//
// Insertion and deletion are constant time operations.
//
// Allocating the set is a constant time operation
// when memory allocation is a constant time operation.
//
// Clearing the set is a constant time operation (unusual!).
//
// Iterating through the set is an O(n) operation, where n
// is the number of items in the set (not O(m)).
//
// The set iterator visits entries in the order they were first
// inserted into the set.  It is safe to add items to the set while
// using an iterator: the iterator will visit indices added to the set
// during the iteration, but will not re-visit indices whose values
// change after visiting.  Thus SparseSet can be a convenient
// implementation of a work queue.
//
// The SparseSet implementation is NOT thread-safe.  It is up to the
// caller to make sure only one thread is accessing the set.  (Typically
// these sets are temporary values and used in situations where speed is
// important.)
//
// The SparseSet interface does not present all the usual STL bells and
// whistles.
//
// Implemented with reference to Briggs & Torczon, An Efficient
// Representation for Sparse Sets, ACM Letters on Programming Languages
// and Systems, Volume 2, Issue 1-4 (March-Dec.  1993), pp.  59-69.
//
// This is a specialization of sparse array; see sparse_array.h.

// IMPLEMENTATION
//
// See sparse_array.h for implementation details.

// Doing this simplifies the logic below.
#ifndef __has_feature
#define __has_feature(x) 0
#endif

#include <assert.h>
#include <stdint.h>
#include <string.h>
#if __has_feature(memory_sanitizer)
#include <sanitizer/msan_interface.h>
#endif
#include <algorithm>
#include <memory>
#include <utility>

namespace re2 {

template<typename Value>
class SparseSetT {
 public:
  SparseSetT();
  explicit SparseSetT(int max_size);
  ~SparseSetT();

  typedef int* iterator;
  typedef const int* const_iterator;

  // Return the number of entries in the set.
  int size() const {
    return size_;
  }

  // Indicate whether the set is empty.
  int empty() const {
    return size_ == 0;
  }

  // Iterate over the set.
  iterator begin() {
    return dense_.get();
  }
  iterator end() {
    return dense_.get() + size_;
  }

  const_iterator begin() const {
    return dense_.get();
  }
  const_iterator end() const {
    return dense_.get() + size_;
  }

  // Change the maximum size of the set.
  // Invalidates all iterators.
  void resize(int max_size);

  // Return the maximum size of the set.
  // Indices can be in the range [0, max_size).
  int max_size() const {
    return max_size_;
  }

  // Clear the set.
  void clear() {
    size_ = 0;
  }

  // Check whether index i is in the set.
  bool contains(int i) const;

  // Comparison function for sorting.
  // Can sort the sparse set so that future iterations
  // will visit indices in increasing order using
  // std::sort(arr.begin(), arr.end(), arr.less);
  static bool less(int a, int b);

 public:
  // Insert index i into the set.
  iterator insert(int i) {
    return InsertInternal(true, i);
  }

  // Insert index i into the set.
  // Fast but unsafe: only use if contains(i) is false.
  iterator insert_new(int i) {
    return InsertInternal(false, i);
  }

 private:
  iterator InsertInternal(bool allow_existing, int i) {
    DebugCheckInvariants();
    if (static_cast<uint32_t>(i) >= static_cast<uint32_t>(max_size_)) {
      assert(false && "illegal index");
      // Semantically, end() would be better here, but we already know
      // the user did something stupid, so begin() insulates them from
      // dereferencing an invalid pointer.
      return begin();
    }
    if (!allow_existing) {
      assert(!contains(i));
      create_index(i);
    } else {
      if (!contains(i))
        create_index(i);
    }
    DebugCheckInvariants();
    return dense_.get() + sparse_[i];
  }

  // Add the index i to the set.
  // Only use if contains(i) is known to be false.
  // This function is private, only intended as a helper
  // for other methods.
  void create_index(int i);

  // In debug mode, verify that some invariant properties of the class
  // are being maintained. This is called at the end of the constructor
  // and at the beginning and end of all public non-const member functions.
  void DebugCheckInvariants() const;

  // Initializes memory for elements [min, max).
  void MaybeInitializeMemory(int min, int max) {
#if __has_feature(memory_sanitizer)
    __msan_unpoison(sparse_.get() + min, (max - min) * sizeof sparse_[0]);
#elif defined(RE2_ON_VALGRIND)
    for (int i = min; i < max; i++) {
      sparse_[i] = 0xababababU;
    }
#endif
  }

  int size_ = 0;
  int max_size_ = 0;
  std::unique_ptr<int[]> sparse_;
  std::unique_ptr<int[]> dense_;
};

template<typename Value>
SparseSetT<Value>::SparseSetT() = default;

// Change the maximum size of the set.
// Invalidates all iterators.
template<typename Value>
void SparseSetT<Value>::resize(int max_size) {
  DebugCheckInvariants();
  if (max_size > max_size_) {
    std::unique_ptr<int[]> a(new int[max_size]);
    if (sparse_) {
      std::copy_n(sparse_.get(), max_size_, a.get());
    }
    sparse_ = std::move(a);

    std::unique_ptr<int[]> b(new int[max_size]);
    if (dense_) {
      std::copy_n(dense_.get(), max_size_, b.get());
    }
    dense_ = std::move(b);

    MaybeInitializeMemory(max_size_, max_size);
  }
  max_size_ = max_size;
  if (size_ > max_size_)
    size_ = max_size_;
  DebugCheckInvariants();
}

// Check whether index i is in the set.
template<typename Value>
bool SparseSetT<Value>::contains(int i) const {
  assert(i >= 0);
  assert(i < max_size_);
  if (static_cast<uint32_t>(i) >= static_cast<uint32_t>(max_size_)) {
    return false;
  }
  // Unsigned comparison avoids checking sparse_[i] < 0.
  return (uint32_t)sparse_[i] < (uint32_t)size_ &&
         dense_[sparse_[i]] == i;
}

template<typename Value>
void SparseSetT<Value>::create_index(int i) {
  assert(!contains(i));
  assert(size_ < max_size_);
  sparse_[i] = size_;
  dense_[size_] = i;
  size_++;
}

template<typename Value> SparseSetT<Value>::SparseSetT(int max_size) {
  sparse_.reset(new int[max_size]);
  dense_.reset(new int[max_size]);
  size_ = 0;
  MaybeInitializeMemory(size_, max_size);
  max_size_ = max_size;
  DebugCheckInvariants();
}

template<typename Value> SparseSetT<Value>::~SparseSetT() {
  DebugCheckInvariants();
}

template<typename Value> void SparseSetT<Value>::DebugCheckInvariants() const {
  assert(0 <= size_);
  assert(size_ <= max_size_);
  assert(size_ == 0 || sparse_ != NULL);
}

// Comparison function for sorting.
template<typename Value> bool SparseSetT<Value>::less(int a, int b) {
  return a < b;
}

typedef SparseSetT<void> SparseSet;

}  // namespace re2

#endif  // UTIL_SPARSE_SET_H_
