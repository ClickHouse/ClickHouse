// Copyright (c) 2005, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---
//
// A dense hashtable is a particular implementation of
// a hashtable: one that is meant to minimize memory allocation.
// It does this by using an array to store all the data.  We
// steal a value from the key space to indicate "empty" array
// elements (ie indices where no item lives) and another to indicate
// "deleted" elements.
//
// (Note it is possible to change the value of the delete key
// on the fly; you can even remove it, though after that point
// the hashtable is insert_only until you set it again.  The empty
// value however can't be changed.)
//
// To minimize allocation and pointer overhead, we use internal
// probing, in which the hashtable is a single table, and collisions
// are resolved by trying to insert again in another bucket.  The
// most cache-efficient internal probing schemes are linear probing
// (which suffers, alas, from clumping) and quadratic probing, which
// is what we implement by default.
//
// Type requirements: value_type is required to be Copy Constructible
// and Default Constructible. It is not required to be (and commonly
// isn't) Assignable.
//
// You probably shouldn't use this code directly.  Use dense_hash_map<>
// or dense_hash_set<> instead.

// You can change the following below:
// HT_OCCUPANCY_PCT      -- how full before we double size
// HT_EMPTY_PCT          -- how empty before we halve size
// HT_MIN_BUCKETS        -- default smallest bucket size
//
// You can also change enlarge_factor (which defaults to
// HT_OCCUPANCY_PCT), and shrink_factor (which defaults to
// HT_EMPTY_PCT) with set_resizing_parameters().
//
// How to decide what values to use?
// shrink_factor's default of .4 * OCCUPANCY_PCT, is probably good.
// HT_MIN_BUCKETS is probably unnecessary since you can specify
// (indirectly) the starting number of buckets at construct-time.
// For enlarge_factor, you can use this chart to try to trade-off
// expected lookup time to the space taken up.  By default, this
// code uses quadratic probing, though you can change it to linear
// via JUMP_ below if you really want to.
//
// From http://www.augustana.ca/~mohrj/courses/1999.fall/csc210/lecture_notes/hashing.html
// NUMBER OF PROBES / LOOKUP       Successful            Unsuccessful
// Quadratic collision resolution   1 - ln(1-L) - L/2    1/(1-L) - L - ln(1-L)
// Linear collision resolution     [1+1/(1-L)]/2         [1+1/(1-L)2]/2
//
// -- enlarge_factor --           0.10  0.50  0.60  0.75  0.80  0.90  0.99
// QUADRATIC COLLISION RES.
//    probes/successful lookup    1.05  1.44  1.62  2.01  2.21  2.85  5.11
//    probes/unsuccessful lookup  1.11  2.19  2.82  4.64  5.81  11.4  103.6
// LINEAR COLLISION RES.
//    probes/successful lookup    1.06  1.5   1.75  2.5   3.0   5.5   50.5
//    probes/unsuccessful lookup  1.12  2.5   3.6   8.5   13.0  50.0  5000.0

#ifndef _DENSEHASHTABLE_H_
#define _DENSEHASHTABLE_H_

#include <sparsehash/internal/sparseconfig.h>
#include <assert.h>
#include <stdio.h>              // for FILE, fwrite, fread
#include <algorithm>            // For swap(), eg
#include <iterator>             // For iterator tags
#include <limits>               // for numeric_limits
#include <memory>               // For uninitialized_fill
#include <utility>              // for pair
#include <sparsehash/internal/hashtable-common.h>
#include <sparsehash/internal/libc_allocator_with_realloc.h>
#include <sparsehash/type_traits.h>
#include <stdexcept>                 // For length_error

_START_GOOGLE_NAMESPACE_

namespace base {   // just to make google->opensource transition easier
using GOOGLE_NAMESPACE::true_type;
using GOOGLE_NAMESPACE::false_type;
using GOOGLE_NAMESPACE::integral_constant;
using GOOGLE_NAMESPACE::is_same;
using GOOGLE_NAMESPACE::remove_const;
}

// The probing method
// Linear probing
// #define JUMP_(key, num_probes)    ( 1 )
// Quadratic probing
#define JUMP_(key, num_probes)    ( num_probes )

// Hashtable class, used to implement the hashed associative containers
// hash_set and hash_map.

// Value: what is stored in the table (each bucket is a Value).
// Key: something in a 1-to-1 correspondence to a Value, that can be used
//      to search for a Value in the table (find() takes a Key).
// HashFcn: Takes a Key and returns an integer, the more unique the better.
// ExtractKey: given a Value, returns the unique Key associated with it.
//             Must inherit from unary_function, or at least have a
//             result_type enum indicating the return type of operator().
// SetKey: given a Value* and a Key, modifies the value such that
//         ExtractKey(value) == key.  We guarantee this is only called
//         with key == deleted_key or key == empty_key.
// EqualKey: Given two Keys, says whether they are the same (that is,
//           if they are both associated with the same Value).
// Alloc: STL allocator to use to allocate memory.

template <class Value, class Key, class HashFcn,
          class ExtractKey, class SetKey, class EqualKey, class Alloc>
class dense_hashtable;

template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
struct dense_hashtable_iterator;

template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
struct dense_hashtable_const_iterator;

// We're just an array, but we need to skip over empty and deleted elements
template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
struct dense_hashtable_iterator {
 private:
  typedef typename A::template rebind<V>::other value_alloc_type;

 public:
  typedef dense_hashtable_iterator<V,K,HF,ExK,SetK,EqK,A>       iterator;
  typedef dense_hashtable_const_iterator<V,K,HF,ExK,SetK,EqK,A> const_iterator;

  typedef std::forward_iterator_tag iterator_category;  // very little defined!
  typedef V value_type;
  typedef typename value_alloc_type::difference_type difference_type;
  typedef typename value_alloc_type::size_type size_type;
  typedef typename value_alloc_type::reference reference;
  typedef typename value_alloc_type::pointer pointer;

  // "Real" constructor and default constructor
  dense_hashtable_iterator(const dense_hashtable<V,K,HF,ExK,SetK,EqK,A> *h,
                           pointer it, pointer it_end, bool advance)
    : ht(h), pos(it), end(it_end)   {
    if (advance)  advance_past_empty_and_deleted();
  }
  dense_hashtable_iterator() { }
  // The default destructor is fine; we don't define one
  // The default operator= is fine; we don't define one

  // Happy dereferencer
  reference operator*() const { return *pos; }
  pointer operator->() const { return &(operator*()); }

  // Arithmetic.  The only hard part is making sure that
  // we're not on an empty or marked-deleted array element
  void advance_past_empty_and_deleted() {
    while ( pos != end && (ht->test_empty(*this) || ht->test_deleted(*this)) )
      ++pos;
  }
  iterator& operator++()   {
    assert(pos != end); ++pos; advance_past_empty_and_deleted(); return *this;
  }
  iterator operator++(int) { iterator tmp(*this); ++*this; return tmp; }

  // Comparison.
  bool operator==(const iterator& it) const { return pos == it.pos; }
  bool operator!=(const iterator& it) const { return pos != it.pos; }


  // The actual data
  const dense_hashtable<V,K,HF,ExK,SetK,EqK,A> *ht;
  pointer pos, end;
};


// Now do it all again, but with const-ness!
template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
struct dense_hashtable_const_iterator {
 private:
  typedef typename A::template rebind<V>::other value_alloc_type;

 public:
  typedef dense_hashtable_iterator<V,K,HF,ExK,SetK,EqK,A>       iterator;
  typedef dense_hashtable_const_iterator<V,K,HF,ExK,SetK,EqK,A> const_iterator;

  typedef std::forward_iterator_tag iterator_category;  // very little defined!
  typedef V value_type;
  typedef typename value_alloc_type::difference_type difference_type;
  typedef typename value_alloc_type::size_type size_type;
  typedef typename value_alloc_type::const_reference reference;
  typedef typename value_alloc_type::const_pointer pointer;

  // "Real" constructor and default constructor
  dense_hashtable_const_iterator(
      const dense_hashtable<V,K,HF,ExK,SetK,EqK,A> *h,
      pointer it, pointer it_end, bool advance)
    : ht(h), pos(it), end(it_end)   {
    if (advance)  advance_past_empty_and_deleted();
  }
  dense_hashtable_const_iterator()
    : ht(NULL), pos(pointer()), end(pointer()) { }
  // This lets us convert regular iterators to const iterators
  dense_hashtable_const_iterator(const iterator &it)
    : ht(it.ht), pos(it.pos), end(it.end) { }
  // The default destructor is fine; we don't define one
  // The default operator= is fine; we don't define one

  // Happy dereferencer
  reference operator*() const { return *pos; }
  pointer operator->() const { return &(operator*()); }

  // Arithmetic.  The only hard part is making sure that
  // we're not on an empty or marked-deleted array element
  void advance_past_empty_and_deleted() {
    while ( pos != end && (ht->test_empty(*this) || ht->test_deleted(*this)) )
      ++pos;
  }
  const_iterator& operator++()   {
    assert(pos != end); ++pos; advance_past_empty_and_deleted(); return *this;
  }
  const_iterator operator++(int) { const_iterator tmp(*this); ++*this; return tmp; }

  // Comparison.
  bool operator==(const const_iterator& it) const { return pos == it.pos; }
  bool operator!=(const const_iterator& it) const { return pos != it.pos; }


  // The actual data
  const dense_hashtable<V,K,HF,ExK,SetK,EqK,A> *ht;
  pointer pos, end;
};

template <class Value, class Key, class HashFcn,
          class ExtractKey, class SetKey, class EqualKey, class Alloc>
class dense_hashtable {
 private:
  typedef typename Alloc::template rebind<Value>::other value_alloc_type;

 public:
  typedef Key key_type;
  typedef Value value_type;
  typedef HashFcn hasher;
  typedef EqualKey key_equal;
  typedef Alloc allocator_type;

  typedef typename value_alloc_type::size_type size_type;
  typedef typename value_alloc_type::difference_type difference_type;
  typedef typename value_alloc_type::reference reference;
  typedef typename value_alloc_type::const_reference const_reference;
  typedef typename value_alloc_type::pointer pointer;
  typedef typename value_alloc_type::const_pointer const_pointer;
  typedef dense_hashtable_iterator<Value, Key, HashFcn,
                                   ExtractKey, SetKey, EqualKey, Alloc>
  iterator;

  typedef dense_hashtable_const_iterator<Value, Key, HashFcn,
                                         ExtractKey, SetKey, EqualKey, Alloc>
  const_iterator;

  // These come from tr1.  For us they're the same as regular iterators.
  typedef iterator local_iterator;
  typedef const_iterator const_local_iterator;

  // How full we let the table get before we resize, by default.
  // Knuth says .8 is good -- higher causes us to probe too much,
  // though it saves memory.
  static const int HT_OCCUPANCY_PCT;  // defined at the bottom of this file

  // How empty we let the table get before we resize lower, by default.
  // (0.0 means never resize lower.)
  // It should be less than OCCUPANCY_PCT / 2 or we thrash resizing
  static const int HT_EMPTY_PCT;      // defined at the bottom of this file

  // Minimum size we're willing to let hashtables be.
  // Must be a power of two, and at least 4.
  // Note, however, that for a given hashtable, the initial size is a
  // function of the first constructor arg, and may be >HT_MIN_BUCKETS.
  static const size_type HT_MIN_BUCKETS = 4;

  // By default, if you don't specify a hashtable size at
  // construction-time, we use this size.  Must be a power of two, and
  // at least HT_MIN_BUCKETS.
  static const size_type HT_DEFAULT_STARTING_BUCKETS = 32;

  // ITERATOR FUNCTIONS
  iterator begin()             { return iterator(this, table,
                                                 table + num_buckets, true); }
  iterator end()               { return iterator(this, table + num_buckets,
                                                 table + num_buckets, true); }
  const_iterator begin() const { return const_iterator(this, table,
                                                       table+num_buckets,true);}
  const_iterator end() const   { return const_iterator(this, table + num_buckets,
                                                       table+num_buckets,true);}

  // These come from tr1 unordered_map.  They iterate over 'bucket' n.
  // We'll just consider bucket n to be the n-th element of the table.
  local_iterator begin(size_type i) {
    return local_iterator(this, table + i, table + i+1, false);
  }
  local_iterator end(size_type i) {
    local_iterator it = begin(i);
    if (!test_empty(i) && !test_deleted(i))
      ++it;
    return it;
  }
  const_local_iterator begin(size_type i) const {
    return const_local_iterator(this, table + i, table + i+1, false);
  }
  const_local_iterator end(size_type i) const {
    const_local_iterator it = begin(i);
    if (!test_empty(i) && !test_deleted(i))
      ++it;
    return it;
  }

  // ACCESSOR FUNCTIONS for the things we templatize on, basically
  hasher hash_funct() const               { return settings; }
  key_equal key_eq() const                { return key_info; }
  allocator_type get_allocator() const {
    return allocator_type(val_info);
  }

  // Accessor function for statistics gathering.
  int num_table_copies() const { return settings.num_ht_copies(); }

 private:
  // Annoyingly, we can't copy values around, because they might have
  // const components (they're probably pair<const X, Y>).  We use
  // explicit destructor invocation and placement new to get around
  // this.  Arg.
  void set_value(pointer dst, const_reference src) {
    dst->~value_type();   // delete the old value, if any
    new(dst) value_type(src);
  }

  void destroy_buckets(size_type first, size_type last) {
    for ( ; first != last; ++first)
      table[first].~value_type();
  }

  // DELETE HELPER FUNCTIONS
  // This lets the user describe a key that will indicate deleted
  // table entries.  This key should be an "impossible" entry --
  // if you try to insert it for real, you won't be able to retrieve it!
  // (NB: while you pass in an entire value, only the key part is looked
  // at.  This is just because I don't know how to assign just a key.)
 private:
  void squash_deleted() {           // gets rid of any deleted entries we have
    if ( num_deleted ) {            // get rid of deleted before writing
      dense_hashtable tmp(*this);   // copying will get rid of deleted
      swap(tmp);                    // now we are tmp
    }
    assert(num_deleted == 0);
  }

  // Test if the given key is the deleted indicator.  Requires
  // num_deleted > 0, for correctness of read(), and because that
  // guarantees that key_info.delkey is valid.
  bool test_deleted_key(const key_type& key) const {
    assert(num_deleted > 0);
    return equals(key_info.delkey, key);
  }

 public:
  void set_deleted_key(const key_type &key) {
    // the empty indicator (if specified) and the deleted indicator
    // must be different
    assert((!settings.use_empty() || !equals(key, get_key(val_info.emptyval)))
           && "Passed the empty-key to set_deleted_key");
    // It's only safe to change what "deleted" means if we purge deleted guys
    squash_deleted();
    settings.set_use_deleted(true);
    key_info.delkey = key;
  }
  void clear_deleted_key() {
    squash_deleted();
    settings.set_use_deleted(false);
  }
  key_type deleted_key() const {
    assert(settings.use_deleted()
           && "Must set deleted key before calling deleted_key");
    return key_info.delkey;
  }

  // These are public so the iterators can use them
  // True if the item at position bucknum is "deleted" marker
  bool test_deleted(size_type bucknum) const {
    // Invariant: !use_deleted() implies num_deleted is 0.
    assert(settings.use_deleted() || num_deleted == 0);
    return num_deleted > 0 && test_deleted_key(get_key(table[bucknum]));
  }
  bool test_deleted(const iterator &it) const {
    // Invariant: !use_deleted() implies num_deleted is 0.
    assert(settings.use_deleted() || num_deleted == 0);
    return num_deleted > 0 && test_deleted_key(get_key(*it));
  }
  bool test_deleted(const const_iterator &it) const {
    // Invariant: !use_deleted() implies num_deleted is 0.
    assert(settings.use_deleted() || num_deleted == 0);
    return num_deleted > 0 && test_deleted_key(get_key(*it));
  }

 private:
  void check_use_deleted(const char* caller) {
    (void)caller;    // could log it if the assert failed
    assert(settings.use_deleted());
  }

  // Set it so test_deleted is true.  true if object didn't used to be deleted.
  bool set_deleted(iterator &it) {
    check_use_deleted("set_deleted()");
    bool retval = !test_deleted(it);
    // &* converts from iterator to value-type.
    set_key(&(*it), key_info.delkey);
    return retval;
  }
  // Set it so test_deleted is false.  true if object used to be deleted.
  bool clear_deleted(iterator &it) {
    check_use_deleted("clear_deleted()");
    // Happens automatically when we assign something else in its place.
    return test_deleted(it);
  }

  // We also allow to set/clear the deleted bit on a const iterator.
  // We allow a const_iterator for the same reason you can delete a
  // const pointer: it's convenient, and semantically you can't use
  // 'it' after it's been deleted anyway, so its const-ness doesn't
  // really matter.
  bool set_deleted(const_iterator &it) {
    check_use_deleted("set_deleted()");
    bool retval = !test_deleted(it);
    set_key(const_cast<pointer>(&(*it)), key_info.delkey);
    return retval;
  }
  // Set it so test_deleted is false.  true if object used to be deleted.
  bool clear_deleted(const_iterator &it) {
    check_use_deleted("clear_deleted()");
    return test_deleted(it);
  }

  // EMPTY HELPER FUNCTIONS
  // This lets the user describe a key that will indicate empty (unused)
  // table entries.  This key should be an "impossible" entry --
  // if you try to insert it for real, you won't be able to retrieve it!
  // (NB: while you pass in an entire value, only the key part is looked
  // at.  This is just because I don't know how to assign just a key.)
 public:
  // These are public so the iterators can use them
  // True if the item at position bucknum is "empty" marker
  bool test_empty(size_type bucknum) const {
    assert(settings.use_empty());  // we always need to know what's empty!
    return equals(get_key(val_info.emptyval), get_key(table[bucknum]));
  }
  bool test_empty(const iterator &it) const {
    assert(settings.use_empty());  // we always need to know what's empty!
    return equals(get_key(val_info.emptyval), get_key(*it));
  }
  bool test_empty(const const_iterator &it) const {
    assert(settings.use_empty());  // we always need to know what's empty!
    return equals(get_key(val_info.emptyval), get_key(*it));
  }

 private:
  void fill_range_with_empty(pointer table_start, pointer table_end) {
    std::uninitialized_fill(table_start, table_end, val_info.emptyval);
  }

 public:
  // TODO(csilvers): change all callers of this to pass in a key instead,
  //                 and take a const key_type instead of const value_type.
  void set_empty_key(const_reference val) {
    // Once you set the empty key, you can't change it
    assert(!settings.use_empty() && "Calling set_empty_key multiple times");
    // The deleted indicator (if specified) and the empty indicator
    // must be different.
    assert((!settings.use_deleted() || !equals(get_key(val), key_info.delkey))
           && "Setting the empty key the same as the deleted key");
    settings.set_use_empty(true);
    set_value(&val_info.emptyval, val);

    assert(!table);                  // must set before first use
    // num_buckets was set in constructor even though table was NULL
    table = val_info.allocate(num_buckets);
    assert(table);
    fill_range_with_empty(table, table + num_buckets);
  }
  // TODO(user): return a key_type rather than a value_type
  value_type empty_key() const {
    assert(settings.use_empty());
    return val_info.emptyval;
  }

  // FUNCTIONS CONCERNING SIZE
 public:
  size_type size() const      { return num_elements - num_deleted; }
  size_type max_size() const  { return val_info.max_size(); }
  bool empty() const          { return size() == 0; }
  size_type bucket_count() const      { return num_buckets; }
  size_type max_bucket_count() const  { return max_size(); }
  size_type nonempty_bucket_count() const { return num_elements; }
  // These are tr1 methods.  Their idea of 'bucket' doesn't map well to
  // what we do.  We just say every bucket has 0 or 1 items in it.
  size_type bucket_size(size_type i) const {
    return begin(i) == end(i) ? 0 : 1;
  }

 private:
  // Because of the above, size_type(-1) is never legal; use it for errors
  static const size_type ILLEGAL_BUCKET = size_type(-1);

  // Used after a string of deletes.  Returns true if we actually shrunk.
  // TODO(csilvers): take a delta so we can take into account inserts
  // done after shrinking.  Maybe make part of the Settings class?
  bool maybe_shrink() {
    assert(num_elements >= num_deleted);
    assert((bucket_count() & (bucket_count()-1)) == 0); // is a power of two
    assert(bucket_count() >= HT_MIN_BUCKETS);
    bool retval = false;

    // If you construct a hashtable with < HT_DEFAULT_STARTING_BUCKETS,
    // we'll never shrink until you get relatively big, and we'll never
    // shrink below HT_DEFAULT_STARTING_BUCKETS.  Otherwise, something
    // like "dense_hash_set<int> x; x.insert(4); x.erase(4);" will
    // shrink us down to HT_MIN_BUCKETS buckets, which is too small.
    const size_type num_remain = num_elements - num_deleted;
    const size_type shrink_threshold = settings.shrink_threshold();
    if (shrink_threshold > 0 && num_remain < shrink_threshold &&
        bucket_count() > HT_DEFAULT_STARTING_BUCKETS) {
      const float shrink_factor = settings.shrink_factor();
      size_type sz = bucket_count() / 2;    // find how much we should shrink
      while (sz > HT_DEFAULT_STARTING_BUCKETS &&
             num_remain < sz * shrink_factor) {
        sz /= 2;                            // stay a power of 2
      }
      dense_hashtable tmp(*this, sz);       // Do the actual resizing
      swap(tmp);                            // now we are tmp
      retval = true;
    }
    settings.set_consider_shrink(false);    // because we just considered it
    return retval;
  }

  // We'll let you resize a hashtable -- though this makes us copy all!
  // When you resize, you say, "make it big enough for this many more elements"
  // Returns true if we actually resized, false if size was already ok.
  bool resize_delta(size_type delta) {
    bool did_resize = false;
    if ( settings.consider_shrink() ) {  // see if lots of deletes happened
      if ( maybe_shrink() )
        did_resize = true;
    }
    if (num_elements >=
        (std::numeric_limits<size_type>::max)() - delta) {
      throw std::length_error("resize overflow");
    }
    if ( bucket_count() >= HT_MIN_BUCKETS &&
         (num_elements + delta) <= settings.enlarge_threshold() )
      return did_resize;                          // we're ok as we are

    // Sometimes, we need to resize just to get rid of all the
    // "deleted" buckets that are clogging up the hashtable.  So when
    // deciding whether to resize, count the deleted buckets (which
    // are currently taking up room).  But later, when we decide what
    // size to resize to, *don't* count deleted buckets, since they
    // get discarded during the resize.
    const size_type needed_size = settings.min_buckets(num_elements + delta, 0);
    if ( needed_size <= bucket_count() )      // we have enough buckets
      return did_resize;

    size_type resize_to =
      settings.min_buckets(num_elements - num_deleted + delta, bucket_count());

    if (resize_to < needed_size &&    // may double resize_to
        resize_to < (std::numeric_limits<size_type>::max)() / 2) {
      // This situation means that we have enough deleted elements,
      // that once we purge them, we won't actually have needed to
      // grow.  But we may want to grow anyway: if we just purge one
      // element, say, we'll have to grow anyway next time we
      // insert.  Might as well grow now, since we're already going
      // through the trouble of copying (in order to purge the
      // deleted elements).
      const size_type target =
          static_cast<size_type>(settings.shrink_size(resize_to*2));
      if (num_elements - num_deleted + delta >= target) {
        // Good, we won't be below the shrink threshhold even if we double.
        resize_to *= 2;
      }
    }
    dense_hashtable tmp(*this, resize_to);
    swap(tmp);                             // now we are tmp
    return true;
  }

  // We require table be not-NULL and empty before calling this.
  void resize_table(size_type /*old_size*/, size_type new_size,
                    base::true_type) {
    table = val_info.realloc_or_die(table, new_size);
  }

  void resize_table(size_type old_size, size_type new_size, base::false_type) {
    val_info.deallocate(table, old_size);
    table = val_info.allocate(new_size);
  }

  // Used to actually do the rehashing when we grow/shrink a hashtable
  void copy_from(const dense_hashtable &ht, size_type min_buckets_wanted) {
    clear_to_size(settings.min_buckets(ht.size(), min_buckets_wanted));

    // We use a normal iterator to get non-deleted bcks from ht
    // We could use insert() here, but since we know there are
    // no duplicates and no deleted items, we can be more efficient
    assert((bucket_count() & (bucket_count()-1)) == 0);      // a power of two
    for ( const_iterator it = ht.begin(); it != ht.end(); ++it ) {
      size_type num_probes = 0;              // how many times we've probed
      size_type bucknum;
      const size_type bucket_count_minus_one = bucket_count() - 1;
      for (bucknum = hash(get_key(*it)) & bucket_count_minus_one;
           !test_empty(bucknum);                               // not empty
           bucknum = (bucknum + JUMP_(key, num_probes)) & bucket_count_minus_one) {
        ++num_probes;
        assert(num_probes < bucket_count()
               && "Hashtable is full: an error in key_equal<> or hash<>");
      }
      set_value(&table[bucknum], *it);       // copies the value to here
      num_elements++;
    }
    settings.inc_num_ht_copies();
  }

  // Required by the spec for hashed associative container
 public:
  // Though the docs say this should be num_buckets, I think it's much
  // more useful as num_elements.  As a special feature, calling with
  // req_elements==0 will cause us to shrink if we can, saving space.
  void resize(size_type req_elements) {       // resize to this or larger
    if ( settings.consider_shrink() || req_elements == 0 )
      maybe_shrink();
    if ( req_elements > num_elements )
      resize_delta(req_elements - num_elements);
  }

  // Get and change the value of shrink_factor and enlarge_factor.  The
  // description at the beginning of this file explains how to choose
  // the values.  Setting the shrink parameter to 0.0 ensures that the
  // table never shrinks.
  void get_resizing_parameters(float* shrink, float* grow) const {
    *shrink = settings.shrink_factor();
    *grow = settings.enlarge_factor();
  }
  void set_resizing_parameters(float shrink, float grow) {
    settings.set_resizing_parameters(shrink, grow);
    settings.reset_thresholds(bucket_count());
  }

  // CONSTRUCTORS -- as required by the specs, we take a size,
  // but also let you specify a hashfunction, key comparator,
  // and key extractor.  We also define a copy constructor and =.
  // DESTRUCTOR -- needs to free the table
  explicit dense_hashtable(size_type expected_max_items_in_table = 0,
                           const HashFcn& hf = HashFcn(),
                           const EqualKey& eql = EqualKey(),
                           const ExtractKey& ext = ExtractKey(),
                           const SetKey& set = SetKey(),
                           const Alloc& alloc = Alloc())
      : settings(hf),
        key_info(ext, set, eql),
        num_deleted(0),
        num_elements(0),
        num_buckets(expected_max_items_in_table == 0
                    ? HT_DEFAULT_STARTING_BUCKETS
                    : settings.min_buckets(expected_max_items_in_table, 0)),
        val_info(alloc_impl<value_alloc_type>(alloc)),
        table(NULL) {
    // table is NULL until emptyval is set.  However, we set num_buckets
    // here so we know how much space to allocate once emptyval is set
    settings.reset_thresholds(bucket_count());
  }

  // As a convenience for resize(), we allow an optional second argument
  // which lets you make this new hashtable a different size than ht
  dense_hashtable(const dense_hashtable& ht,
                  size_type min_buckets_wanted = HT_DEFAULT_STARTING_BUCKETS)
      : settings(ht.settings),
        key_info(ht.key_info),
        num_deleted(0),
        num_elements(0),
        num_buckets(0),
        val_info(ht.val_info),
        table(NULL) {
    if (!ht.settings.use_empty()) {
      // If use_empty isn't set, copy_from will crash, so we do our own copying.
      assert(ht.empty());
      num_buckets = settings.min_buckets(ht.size(), min_buckets_wanted);
      settings.reset_thresholds(bucket_count());
      return;
    }
    settings.reset_thresholds(bucket_count());
    copy_from(ht, min_buckets_wanted);   // copy_from() ignores deleted entries
  }

  dense_hashtable& operator= (const dense_hashtable& ht) {
    if (&ht == this)  return *this;        // don't copy onto ourselves
    if (!ht.settings.use_empty()) {
      assert(ht.empty());
      dense_hashtable empty_table(ht);  // empty table with ht's thresholds
      this->swap(empty_table);
      return *this;
    }
    settings = ht.settings;
    key_info = ht.key_info;
    set_value(&val_info.emptyval, ht.val_info.emptyval);
    // copy_from() calls clear and sets num_deleted to 0 too
    copy_from(ht, HT_MIN_BUCKETS);
    // we purposefully don't copy the allocator, which may not be copyable
    return *this;
  }

  ~dense_hashtable() {
    if (table) {
      destroy_buckets(0, num_buckets);
      val_info.deallocate(table, num_buckets);
    }
  }

  // Many STL algorithms use swap instead of copy constructors
  void swap(dense_hashtable& ht) {
    std::swap(settings, ht.settings);
    std::swap(key_info, ht.key_info);
    std::swap(num_deleted, ht.num_deleted);
    std::swap(num_elements, ht.num_elements);
    std::swap(num_buckets, ht.num_buckets);
    { value_type tmp;     // for annoying reasons, swap() doesn't work
      set_value(&tmp, val_info.emptyval);
      set_value(&val_info.emptyval, ht.val_info.emptyval);
      set_value(&ht.val_info.emptyval, tmp);
    }
    std::swap(table, ht.table);
    settings.reset_thresholds(bucket_count());  // also resets consider_shrink
    ht.settings.reset_thresholds(ht.bucket_count());
    // we purposefully don't swap the allocator, which may not be swap-able
  }

 private:
  void clear_to_size(size_type new_num_buckets) {
    if (!table) {
      table = val_info.allocate(new_num_buckets);
    } else {
      destroy_buckets(0, num_buckets);
      if (new_num_buckets != num_buckets) {   // resize, if necessary
        typedef base::integral_constant<bool,
            base::is_same<value_alloc_type,
                          libc_allocator_with_realloc<value_type> >::value>
            realloc_ok;
        resize_table(num_buckets, new_num_buckets, realloc_ok());
      }
    }
    assert(table);
    fill_range_with_empty(table, table + new_num_buckets);
    num_elements = 0;
    num_deleted = 0;
    num_buckets = new_num_buckets;          // our new size
    settings.reset_thresholds(bucket_count());
  }

 public:
  // It's always nice to be able to clear a table without deallocating it
  void clear() {
    // If the table is already empty, and the number of buckets is
    // already as we desire, there's nothing to do.
    const size_type new_num_buckets = settings.min_buckets(0, 0);
    if (num_elements == 0 && new_num_buckets == num_buckets) {
      return;
    }
    clear_to_size(new_num_buckets);
  }

  // Clear the table without resizing it.
  // Mimicks the stl_hashtable's behaviour when clear()-ing in that it
  // does not modify the bucket count
  void clear_no_resize() {
    if (num_elements > 0) {
      assert(table);
      destroy_buckets(0, num_buckets);
      fill_range_with_empty(table, table + num_buckets);
    }
    // don't consider to shrink before another erase()
    settings.reset_thresholds(bucket_count());
    num_elements = 0;
    num_deleted = 0;
  }

  // LOOKUP ROUTINES
 private:
  // Returns a pair of positions: 1st where the object is, 2nd where
  // it would go if you wanted to insert it.  1st is ILLEGAL_BUCKET
  // if object is not found; 2nd is ILLEGAL_BUCKET if it is.
  // Note: because of deletions where-to-insert is not trivial: it's the
  // first deleted bucket we see, as long as we don't find the key later
  std::pair<size_type, size_type> find_position(const key_type &key) const {
    size_type num_probes = 0;              // how many times we've probed
    const size_type bucket_count_minus_one = bucket_count() - 1;
    size_type bucknum = hash(key) & bucket_count_minus_one;
    size_type insert_pos = ILLEGAL_BUCKET; // where we would insert
    while ( 1 ) {                          // probe until something happens
      if ( test_empty(bucknum) ) {         // bucket is empty
        if ( insert_pos == ILLEGAL_BUCKET )   // found no prior place to insert
          return std::pair<size_type,size_type>(ILLEGAL_BUCKET, bucknum);
        else
          return std::pair<size_type,size_type>(ILLEGAL_BUCKET, insert_pos);

      } else if ( test_deleted(bucknum) ) {// keep searching, but mark to insert
        if ( insert_pos == ILLEGAL_BUCKET )
          insert_pos = bucknum;

      } else if ( equals(key, get_key(table[bucknum])) ) {
        return std::pair<size_type,size_type>(bucknum, ILLEGAL_BUCKET);
      }
      ++num_probes;                        // we're doing another probe
      bucknum = (bucknum + JUMP_(key, num_probes)) & bucket_count_minus_one;
      assert(num_probes < bucket_count()
             && "Hashtable is full: an error in key_equal<> or hash<>");
    }
  }

 public:

  iterator find(const key_type& key) {
    if ( size() == 0 ) return end();
    std::pair<size_type, size_type> pos = find_position(key);
    if ( pos.first == ILLEGAL_BUCKET )     // alas, not there
      return end();
    else
      return iterator(this, table + pos.first, table + num_buckets, false);
  }

  const_iterator find(const key_type& key) const {
    if ( size() == 0 ) return end();
    std::pair<size_type, size_type> pos = find_position(key);
    if ( pos.first == ILLEGAL_BUCKET )     // alas, not there
      return end();
    else
      return const_iterator(this, table + pos.first, table+num_buckets, false);
  }

  // This is a tr1 method: the bucket a given key is in, or what bucket
  // it would be put in, if it were to be inserted.  Shrug.
  size_type bucket(const key_type& key) const {
    std::pair<size_type, size_type> pos = find_position(key);
    return pos.first == ILLEGAL_BUCKET ? pos.second : pos.first;
  }

  // Counts how many elements have key key.  For maps, it's either 0 or 1.
  size_type count(const key_type &key) const {
    std::pair<size_type, size_type> pos = find_position(key);
    return pos.first == ILLEGAL_BUCKET ? 0 : 1;
  }

  // Likewise, equal_range doesn't really make sense for us.  Oh well.
  std::pair<iterator,iterator> equal_range(const key_type& key) {
    iterator pos = find(key);      // either an iterator or end
    if (pos == end()) {
      return std::pair<iterator,iterator>(pos, pos);
    } else {
      const iterator startpos = pos++;
      return std::pair<iterator,iterator>(startpos, pos);
    }
  }
  std::pair<const_iterator,const_iterator> equal_range(const key_type& key)
      const {
    const_iterator pos = find(key);      // either an iterator or end
    if (pos == end()) {
      return std::pair<const_iterator,const_iterator>(pos, pos);
    } else {
      const const_iterator startpos = pos++;
      return std::pair<const_iterator,const_iterator>(startpos, pos);
    }
  }


  // INSERTION ROUTINES
 private:
  // Private method used by insert_noresize and find_or_insert.
  iterator insert_at(const_reference obj, size_type pos) {
    if (size() >= max_size()) {
      throw std::length_error("insert overflow");
    }
    if ( test_deleted(pos) ) {      // just replace if it's been del.
      // shrug: shouldn't need to be const.
      const_iterator delpos(this, table + pos, table + num_buckets, false);
      clear_deleted(delpos);
      assert( num_deleted > 0);
      --num_deleted;                // used to be, now it isn't
    } else {
      ++num_elements;               // replacing an empty bucket
    }
    set_value(&table[pos], obj);
    return iterator(this, table + pos, table + num_buckets, false);
  }

  // If you know *this is big enough to hold obj, use this routine
  std::pair<iterator, bool> insert_noresize(const_reference obj) {
    // First, double-check we're not inserting delkey or emptyval
    assert((!settings.use_empty() || !equals(get_key(obj),
                                             get_key(val_info.emptyval)))
           && "Inserting the empty key");
    assert((!settings.use_deleted() || !equals(get_key(obj), key_info.delkey))
           && "Inserting the deleted key");
    const std::pair<size_type,size_type> pos = find_position(get_key(obj));
    if ( pos.first != ILLEGAL_BUCKET) {      // object was already there
      return std::pair<iterator,bool>(iterator(this, table + pos.first,
                                          table + num_buckets, false),
                                 false);          // false: we didn't insert
    } else {                                 // pos.second says where to put it
      return std::pair<iterator,bool>(insert_at(obj, pos.second), true);
    }
  }

  // Specializations of insert(it, it) depending on the power of the iterator:
  // (1) Iterator supports operator-, resize before inserting
  template <class ForwardIterator>
  void insert(ForwardIterator f, ForwardIterator l, std::forward_iterator_tag) {
    size_t dist = std::distance(f, l);
    if (dist >= (std::numeric_limits<size_type>::max)()) {
      throw std::length_error("insert-range overflow");
    }
    resize_delta(static_cast<size_type>(dist));
    for ( ; dist > 0; --dist, ++f) {
      insert_noresize(*f);
    }
  }

  // (2) Arbitrary iterator, can't tell how much to resize
  template <class InputIterator>
  void insert(InputIterator f, InputIterator l, std::input_iterator_tag) {
    for ( ; f != l; ++f)
      insert(*f);
  }

 public:
  // This is the normal insert routine, used by the outside world
  std::pair<iterator, bool> insert(const_reference obj) {
    resize_delta(1);                      // adding an object, grow if need be
    return insert_noresize(obj);
  }

  // When inserting a lot at a time, we specialize on the type of iterator
  template <class InputIterator>
  void insert(InputIterator f, InputIterator l) {
    // specializes on iterator type
    insert(f, l,
           typename std::iterator_traits<InputIterator>::iterator_category());
  }

  // DefaultValue is a functor that takes a key and returns a value_type
  // representing the default value to be inserted if none is found.
  template <class DefaultValue>
  value_type& find_or_insert(const key_type& key) {
    // First, double-check we're not inserting emptykey or delkey
    assert((!settings.use_empty() || !equals(key, get_key(val_info.emptyval)))
           && "Inserting the empty key");
    assert((!settings.use_deleted() || !equals(key, key_info.delkey))
           && "Inserting the deleted key");
    const std::pair<size_type,size_type> pos = find_position(key);
    DefaultValue default_value;
    if ( pos.first != ILLEGAL_BUCKET) {  // object was already there
      return table[pos.first];
    } else if (resize_delta(1)) {        // needed to rehash to make room
      // Since we resized, we can't use pos, so recalculate where to insert.
      return *insert_noresize(default_value(key)).first;
    } else {                             // no need to rehash, insert right here
      return *insert_at(default_value(key), pos.second);
    }
  }


  // DELETION ROUTINES
  size_type erase(const key_type& key) {
    // First, double-check we're not trying to erase delkey or emptyval.
    assert((!settings.use_empty() || !equals(key, get_key(val_info.emptyval)))
           && "Erasing the empty key");
    assert((!settings.use_deleted() || !equals(key, key_info.delkey))
           && "Erasing the deleted key");
    const_iterator pos = find(key);   // shrug: shouldn't need to be const
    if ( pos != end() ) {
      assert(!test_deleted(pos));  // or find() shouldn't have returned it
      set_deleted(pos);
      ++num_deleted;
      settings.set_consider_shrink(true); // will think about shrink after next insert
      return 1;                    // because we deleted one thing
    } else {
      return 0;                    // because we deleted nothing
    }
  }

  // We return the iterator past the deleted item.
  void erase(iterator pos) {
    if ( pos == end() ) return;    // sanity check
    if ( set_deleted(pos) ) {      // true if object has been newly deleted
      ++num_deleted;
      settings.set_consider_shrink(true); // will think about shrink after next insert
    }
  }

  void erase(iterator f, iterator l) {
    for ( ; f != l; ++f) {
      if ( set_deleted(f)  )       // should always be true
        ++num_deleted;
    }
    settings.set_consider_shrink(true); // will think about shrink after next insert
  }

  // We allow you to erase a const_iterator just like we allow you to
  // erase an iterator.  This is in parallel to 'delete': you can delete
  // a const pointer just like a non-const pointer.  The logic is that
  // you can't use the object after it's erased anyway, so it doesn't matter
  // if it's const or not.
  void erase(const_iterator pos) {
    if ( pos == end() ) return;    // sanity check
    if ( set_deleted(pos) ) {      // true if object has been newly deleted
      ++num_deleted;
      settings.set_consider_shrink(true); // will think about shrink after next insert
    }
  }
  void erase(const_iterator f, const_iterator l) {
    for ( ; f != l; ++f) {
      if ( set_deleted(f)  )       // should always be true
        ++num_deleted;
    }
    settings.set_consider_shrink(true);   // will think about shrink after next insert
  }


  // COMPARISON
  bool operator==(const dense_hashtable& ht) const {
    if (size() != ht.size()) {
      return false;
    } else if (this == &ht) {
      return true;
    } else {
      // Iterate through the elements in "this" and see if the
      // corresponding element is in ht
      for ( const_iterator it = begin(); it != end(); ++it ) {
        const_iterator it2 = ht.find(get_key(*it));
        if ((it2 == ht.end()) || (*it != *it2)) {
          return false;
        }
      }
      return true;
    }
  }
  bool operator!=(const dense_hashtable& ht) const {
    return !(*this == ht);
  }


  // I/O
  // We support reading and writing hashtables to disk.  Alas, since
  // I don't know how to write a hasher or key_equal, you have to make
  // sure everything but the table is the same.  We compact before writing.
 private:
  // Every time the disk format changes, this should probably change too
  typedef unsigned long MagicNumberType;
  static const MagicNumberType MAGIC_NUMBER = 0x13578642;

 public:
  // I/O -- this is an add-on for writing hash table to disk
  //
  // INPUT and OUTPUT must be either a FILE, *or* a C++ stream
  //    (istream, ostream, etc) *or* a class providing
  //    Read(void*, size_t) and Write(const void*, size_t)
  //    (respectively), which writes a buffer into a stream
  //    (which the INPUT/OUTPUT instance presumably owns).

  typedef sparsehash_internal::pod_serializer<value_type> NopointerSerializer;

  // ValueSerializer: a functor.  operator()(OUTPUT*, const value_type&)
  template <typename ValueSerializer, typename OUTPUT>
  bool serialize(ValueSerializer serializer, OUTPUT *fp) {
    squash_deleted();           // so we don't have to worry about delkey
    if ( !sparsehash_internal::write_bigendian_number(fp, MAGIC_NUMBER, 4) )
      return false;
    if ( !sparsehash_internal::write_bigendian_number(fp, num_buckets, 8) )
      return false;
    if ( !sparsehash_internal::write_bigendian_number(fp, num_elements, 8) )
      return false;
    // Now write a bitmap of non-empty buckets.
    for ( size_type i = 0; i < num_buckets; i += 8 ) {
      unsigned char bits = 0;
      for ( int bit = 0; bit < 8; ++bit ) {
        if ( i + bit < num_buckets && !test_empty(i + bit) )
          bits |= (1 << bit);
      }
      if ( !sparsehash_internal::write_data(fp, &bits, sizeof(bits)) )
        return false;
      for ( int bit = 0; bit < 8; ++bit ) {
        if ( bits & (1 << bit) ) {
          if ( !serializer(fp, table[i + bit]) ) return false;
        }
      }
    }
    return true;
  }

  // INPUT: anything we've written an overload of read_data() for.
  // ValueSerializer: a functor.  operator()(INPUT*, value_type*)
  template <typename ValueSerializer, typename INPUT>
  bool unserialize(ValueSerializer serializer, INPUT *fp) {
    assert(settings.use_empty() && "empty_key not set for read");

    clear();                        // just to be consistent
    MagicNumberType magic_read;
    if ( !sparsehash_internal::read_bigendian_number(fp, &magic_read, 4) )
      return false;
    if ( magic_read != MAGIC_NUMBER ) {
      return false;
    }
    size_type new_num_buckets;
    if ( !sparsehash_internal::read_bigendian_number(fp, &new_num_buckets, 8) )
      return false;
    clear_to_size(new_num_buckets);
    if ( !sparsehash_internal::read_bigendian_number(fp, &num_elements, 8) )
      return false;

    // Read the bitmap of non-empty buckets.
    for (size_type i = 0; i < num_buckets; i += 8) {
      unsigned char bits;
      if ( !sparsehash_internal::read_data(fp, &bits, sizeof(bits)) )
        return false;
      for ( int bit = 0; bit < 8; ++bit ) {
        if ( i + bit < num_buckets && (bits & (1 << bit)) ) {  // not empty
          if ( !serializer(fp, &table[i + bit]) ) return false;
        }
      }
    }
    return true;
  }

 private:
  template <class A>
  class alloc_impl : public A {
   public:
    typedef typename A::pointer pointer;
    typedef typename A::size_type size_type;

    // Convert a normal allocator to one that has realloc_or_die()
    alloc_impl(const A& a) : A(a) { }

    // realloc_or_die should only be used when using the default
    // allocator (libc_allocator_with_realloc).
    pointer realloc_or_die(pointer /*ptr*/, size_type /*n*/) {
      fprintf(stderr, "realloc_or_die is only supported for "
                      "libc_allocator_with_realloc\n");
      exit(1);
      return NULL;
    }
  };

  // A template specialization of alloc_impl for
  // libc_allocator_with_realloc that can handle realloc_or_die.
  template <class A>
  class alloc_impl<libc_allocator_with_realloc<A> >
      : public libc_allocator_with_realloc<A> {
   public:
    typedef typename libc_allocator_with_realloc<A>::pointer pointer;
    typedef typename libc_allocator_with_realloc<A>::size_type size_type;

    alloc_impl(const libc_allocator_with_realloc<A>& a)
        : libc_allocator_with_realloc<A>(a) { }

    pointer realloc_or_die(pointer ptr, size_type n) {
      pointer retval = this->reallocate(ptr, n);
      if (retval == NULL) {
        fprintf(stderr, "sparsehash: FATAL ERROR: failed to reallocate "
                "%lu elements for ptr %p", static_cast<unsigned long>(n), ptr);
        exit(1);
      }
      return retval;
    }
  };

  // Package allocator with emptyval to eliminate memory needed for
  // the zero-size allocator.
  // If new fields are added to this class, we should add them to
  // operator= and swap.
  class ValInfo : public alloc_impl<value_alloc_type> {
   public:
    typedef typename alloc_impl<value_alloc_type>::value_type value_type;

    ValInfo(const alloc_impl<value_alloc_type>& a)
        : alloc_impl<value_alloc_type>(a), emptyval() { }
    ValInfo(const ValInfo& v)
        : alloc_impl<value_alloc_type>(v), emptyval(v.emptyval) { }

    value_type emptyval;    // which key marks unused entries
  };


  // Package functors with another class to eliminate memory needed for
  // zero-size functors.  Since ExtractKey and hasher's operator() might
  // have the same function signature, they must be packaged in
  // different classes.
  struct Settings :
      sparsehash_internal::sh_hashtable_settings<key_type, hasher,
                                                 size_type, HT_MIN_BUCKETS> {
    explicit Settings(const hasher& hf)
        : sparsehash_internal::sh_hashtable_settings<key_type, hasher,
                                                     size_type, HT_MIN_BUCKETS>(
            hf, HT_OCCUPANCY_PCT / 100.0f, HT_EMPTY_PCT / 100.0f) {}
  };

  // Packages ExtractKey and SetKey functors.
  class KeyInfo : public ExtractKey, public SetKey, public EqualKey {
   public:
    KeyInfo(const ExtractKey& ek, const SetKey& sk, const EqualKey& eq)
        : ExtractKey(ek),
          SetKey(sk),
          EqualKey(eq) {
    }

    // We want to return the exact same type as ExtractKey: Key or const Key&
    typename ExtractKey::result_type get_key(const_reference v) const {
      return ExtractKey::operator()(v);
    }
    void set_key(pointer v, const key_type& k) const {
      SetKey::operator()(v, k);
    }
    bool equals(const key_type& a, const key_type& b) const {
      return EqualKey::operator()(a, b);
    }

    // Which key marks deleted entries.
    // TODO(csilvers): make a pointer, and get rid of use_deleted (benchmark!)
    typename base::remove_const<key_type>::type delkey;
  };

  // Utility functions to access the templated operators
  size_type hash(const key_type& v) const {
    return settings.hash(v);
  }
  bool equals(const key_type& a, const key_type& b) const {
    return key_info.equals(a, b);
  }
  typename ExtractKey::result_type get_key(const_reference v) const {
    return key_info.get_key(v);
  }
  void set_key(pointer v, const key_type& k) const {
    key_info.set_key(v, k);
  }

 private:
  // Actual data
  Settings settings;
  KeyInfo key_info;

  size_type num_deleted;  // how many occupied buckets are marked deleted
  size_type num_elements;
  size_type num_buckets;
  ValInfo val_info;       // holds emptyval, and also the allocator
  pointer table;
};


// We need a global swap as well
template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
inline void swap(dense_hashtable<V,K,HF,ExK,SetK,EqK,A> &x,
                 dense_hashtable<V,K,HF,ExK,SetK,EqK,A> &y) {
  x.swap(y);
}

#undef JUMP_

template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
const typename dense_hashtable<V,K,HF,ExK,SetK,EqK,A>::size_type
  dense_hashtable<V,K,HF,ExK,SetK,EqK,A>::ILLEGAL_BUCKET;

// How full we let the table get before we resize.  Knuth says .8 is
// good -- higher causes us to probe too much, though saves memory.
// However, we go with .5, getting better performance at the cost of
// more space (a trade-off densehashtable explicitly chooses to make).
// Feel free to play around with different values, though, via
// max_load_factor() and/or set_resizing_parameters().
template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
const int dense_hashtable<V,K,HF,ExK,SetK,EqK,A>::HT_OCCUPANCY_PCT = 50;

// How empty we let the table get before we resize lower.
// It should be less than OCCUPANCY_PCT / 2 or we thrash resizing.
template <class V, class K, class HF, class ExK, class SetK, class EqK, class A>
const int dense_hashtable<V,K,HF,ExK,SetK,EqK,A>::HT_EMPTY_PCT
  = static_cast<int>(0.4 *
                     dense_hashtable<V,K,HF,ExK,SetK,EqK,A>::HT_OCCUPANCY_PCT);

_END_GOOGLE_NAMESPACE_

#endif /* _DENSEHASHTABLE_H_ */
