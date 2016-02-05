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
// This is just a very thin wrapper over sparsehashtable.h, just
// like sgi stl's stl_hash_map is a very thin wrapper over
// stl_hashtable.  The major thing we define is operator[], because
// we have a concept of a data_type which stl_hashtable doesn't
// (it only has a key and a value).
//
// We adhere mostly to the STL semantics for hash-map.  One important
// exception is that insert() may invalidate iterators entirely -- STL
// semantics are that insert() may reorder iterators, but they all
// still refer to something valid in the hashtable.  Not so for us.
// Likewise, insert() may invalidate pointers into the hashtable.
// (Whether insert invalidates iterators and pointers depends on
// whether it results in a hashtable resize).  On the plus side,
// delete() doesn't invalidate iterators or pointers at all, or even
// change the ordering of elements.
//
// Here are a few "power user" tips:
//
//    1) set_deleted_key():
//         Unlike STL's hash_map, if you want to use erase() you
//         *must* call set_deleted_key() after construction.
//
//    2) resize(0):
//         When an item is deleted, its memory isn't freed right
//         away.  This is what allows you to iterate over a hashtable
//         and call erase() without invalidating the iterator.
//         To force the memory to be freed, call resize(0).
//         For tr1 compatibility, this can also be called as rehash(0).
//
//    3) min_load_factor(0.0)
//         Setting the minimum load factor to 0.0 guarantees that
//         the hash table will never shrink.
//
// Roughly speaking:
//   (1) dense_hash_map: fastest, uses the most memory unless entries are small
//   (2) sparse_hash_map: slowest, uses the least memory
//   (3) hash_map / unordered_map (STL): in the middle
//
// Typically I use sparse_hash_map when I care about space and/or when
// I need to save the hashtable on disk.  I use hash_map otherwise.  I
// don't personally use dense_hash_map ever; some people use it for
// small maps with lots of lookups.
//
// - dense_hash_map has, typically, about 78% memory overhead (if your
//   data takes up X bytes, the hash_map uses .78X more bytes in overhead).
// - sparse_hash_map has about 4 bits overhead per entry.
// - sparse_hash_map can be 3-7 times slower than the others for lookup and,
//   especially, inserts.  See time_hash_map.cc for details.
//
// See /usr/(local/)?doc/sparsehash-*/sparse_hash_map.html
// for information about how to use this class.

#ifndef _SPARSE_HASH_MAP_H_
#define _SPARSE_HASH_MAP_H_

#include <sparsehash/internal/sparseconfig.h>
#include <algorithm>                        // needed by stl_alloc
#include <functional>                       // for equal_to<>, select1st<>, etc
#include <memory>                           // for alloc
#include <utility>                          // for pair<>
#include <sparsehash/internal/libc_allocator_with_realloc.h>
#include <sparsehash/internal/sparsehashtable.h>       // IWYU pragma: export
#include HASH_FUN_H                 // for hash<>
_START_GOOGLE_NAMESPACE_

template <class Key, class T,
          class HashFcn = SPARSEHASH_HASH<Key>,   // defined in sparseconfig.h
          class EqualKey = std::equal_to<Key>,
          class Alloc = libc_allocator_with_realloc<std::pair<const Key, T> > >
class sparse_hash_map {
 private:
  // Apparently select1st is not stl-standard, so we define our own
  struct SelectKey {
    typedef const Key& result_type;
    const Key& operator()(const std::pair<const Key, T>& p) const {
      return p.first;
    }
  };
  struct SetKey {
    void operator()(std::pair<const Key, T>* value, const Key& new_key) const {
      *const_cast<Key*>(&value->first) = new_key;
      // It would be nice to clear the rest of value here as well, in
      // case it's taking up a lot of memory.  We do this by clearing
      // the value.  This assumes T has a zero-arg constructor!
      value->second = T();
    }
  };
  // For operator[].
  struct DefaultValue {
    std::pair<const Key, T> operator()(const Key& key) {
      return std::make_pair(key, T());
    }
  };

  // The actual data
  typedef sparse_hashtable<std::pair<const Key, T>, Key, HashFcn, SelectKey,
                           SetKey, EqualKey, Alloc> ht;
  ht rep;

 public:
  typedef typename ht::key_type key_type;
  typedef T data_type;
  typedef T mapped_type;
  typedef typename ht::value_type value_type;
  typedef typename ht::hasher hasher;
  typedef typename ht::key_equal key_equal;
  typedef Alloc allocator_type;

  typedef typename ht::size_type size_type;
  typedef typename ht::difference_type difference_type;
  typedef typename ht::pointer pointer;
  typedef typename ht::const_pointer const_pointer;
  typedef typename ht::reference reference;
  typedef typename ht::const_reference const_reference;

  typedef typename ht::iterator iterator;
  typedef typename ht::const_iterator const_iterator;
  typedef typename ht::local_iterator local_iterator;
  typedef typename ht::const_local_iterator const_local_iterator;

  // Iterator functions
  iterator begin()                               { return rep.begin(); }
  iterator end()                                 { return rep.end(); }
  const_iterator begin() const                   { return rep.begin(); }
  const_iterator end() const                     { return rep.end(); }

  // These come from tr1's unordered_map. For us, a bucket has 0 or 1 elements.
  local_iterator begin(size_type i)              { return rep.begin(i); }
  local_iterator end(size_type i)                { return rep.end(i); }
  const_local_iterator begin(size_type i) const  { return rep.begin(i); }
  const_local_iterator end(size_type i) const    { return rep.end(i); }

  // Accessor functions
  allocator_type get_allocator() const           { return rep.get_allocator(); }
  hasher hash_funct() const                      { return rep.hash_funct(); }
  hasher hash_function() const                   { return hash_funct(); }
  key_equal key_eq() const                       { return rep.key_eq(); }


  // Constructors
  explicit sparse_hash_map(size_type expected_max_items_in_table = 0,
                           const hasher& hf = hasher(),
                           const key_equal& eql = key_equal(),
                           const allocator_type& alloc = allocator_type())
    : rep(expected_max_items_in_table, hf, eql, SelectKey(), SetKey(), alloc) {
  }

  template <class InputIterator>
  sparse_hash_map(InputIterator f, InputIterator l,
                  size_type expected_max_items_in_table = 0,
                  const hasher& hf = hasher(),
                  const key_equal& eql = key_equal(),
                  const allocator_type& alloc = allocator_type())
    : rep(expected_max_items_in_table, hf, eql, SelectKey(), SetKey(), alloc) {
    rep.insert(f, l);
  }
  // We use the default copy constructor
  // We use the default operator=()
  // We use the default destructor

  void clear()                        { rep.clear(); }
  void swap(sparse_hash_map& hs)      { rep.swap(hs.rep); }


  // Functions concerning size
  size_type size() const              { return rep.size(); }
  size_type max_size() const          { return rep.max_size(); }
  bool empty() const                  { return rep.empty(); }
  size_type bucket_count() const      { return rep.bucket_count(); }
  size_type max_bucket_count() const  { return rep.max_bucket_count(); }

  // These are tr1 methods.  bucket() is the bucket the key is or would be in.
  size_type bucket_size(size_type i) const    { return rep.bucket_size(i); }
  size_type bucket(const key_type& key) const { return rep.bucket(key); }
  float load_factor() const {
    return size() * 1.0f / bucket_count();
  }
  float max_load_factor() const {
    float shrink, grow;
    rep.get_resizing_parameters(&shrink, &grow);
    return grow;
  }
  void max_load_factor(float new_grow) {
    float shrink, grow;
    rep.get_resizing_parameters(&shrink, &grow);
    rep.set_resizing_parameters(shrink, new_grow);
  }
  // These aren't tr1 methods but perhaps ought to be.
  float min_load_factor() const {
    float shrink, grow;
    rep.get_resizing_parameters(&shrink, &grow);
    return shrink;
  }
  void min_load_factor(float new_shrink) {
    float shrink, grow;
    rep.get_resizing_parameters(&shrink, &grow);
    rep.set_resizing_parameters(new_shrink, grow);
  }
  // Deprecated; use min_load_factor() or max_load_factor() instead.
  void set_resizing_parameters(float shrink, float grow) {
    rep.set_resizing_parameters(shrink, grow);
  }

  void resize(size_type hint)         { rep.resize(hint); }
  void rehash(size_type hint)         { resize(hint); }      // the tr1 name

  // Lookup routines
  iterator find(const key_type& key)                 { return rep.find(key); }
  const_iterator find(const key_type& key) const     { return rep.find(key); }

  data_type& operator[](const key_type& key) {       // This is our value-add!
    // If key is in the hashtable, returns find(key)->second,
    // otherwise returns insert(value_type(key, T()).first->second.
    // Note it does not create an empty T unless the find fails.
    return rep.template find_or_insert<DefaultValue>(key).second;
  }

  size_type count(const key_type& key) const         { return rep.count(key); }

  std::pair<iterator, iterator> equal_range(const key_type& key) {
    return rep.equal_range(key);
  }
  std::pair<const_iterator, const_iterator> equal_range(const key_type& key)
      const {
    return rep.equal_range(key);
  }

  // Insertion routines
  std::pair<iterator, bool> insert(const value_type& obj) {
    return rep.insert(obj);
  }
  template <class InputIterator> void insert(InputIterator f, InputIterator l) {
    rep.insert(f, l);
  }
  void insert(const_iterator f, const_iterator l) {
    rep.insert(f, l);
  }
  // Required for std::insert_iterator; the passed-in iterator is ignored.
  iterator insert(iterator, const value_type& obj) {
    return insert(obj).first;
  }

  // Deletion routines
  // THESE ARE NON-STANDARD!  I make you specify an "impossible" key
  // value to identify deleted buckets.  You can change the key as
  // time goes on, or get rid of it entirely to be insert-only.
  void set_deleted_key(const key_type& key)   {
    rep.set_deleted_key(key);
  }
  void clear_deleted_key()                    { rep.clear_deleted_key(); }
  key_type deleted_key() const                { return rep.deleted_key(); }

  // These are standard
  size_type erase(const key_type& key)               { return rep.erase(key); }
  void erase(iterator it)                            { rep.erase(it); }
  void erase(iterator f, iterator l)                 { rep.erase(f, l); }


  // Comparison
  bool operator==(const sparse_hash_map& hs) const   { return rep == hs.rep; }
  bool operator!=(const sparse_hash_map& hs) const   { return rep != hs.rep; }


  // I/O -- this is an add-on for writing metainformation to disk
  //
  // For maximum flexibility, this does not assume a particular
  // file type (though it will probably be a FILE *).  We just pass
  // the fp through to rep.

  // If your keys and values are simple enough, you can pass this
  // serializer to serialize()/unserialize().  "Simple enough" means
  // value_type is a POD type that contains no pointers.  Note,
  // however, we don't try to normalize endianness.
  typedef typename ht::NopointerSerializer NopointerSerializer;

  // serializer: a class providing operator()(OUTPUT*, const value_type&)
  //    (writing value_type to OUTPUT).  You can specify a
  //    NopointerSerializer object if appropriate (see above).
  // fp: either a FILE*, OR an ostream*/subclass_of_ostream*, OR a
  //    pointer to a class providing size_t Write(const void*, size_t),
  //    which writes a buffer into a stream (which fp presumably
  //    owns) and returns the number of bytes successfully written.
  //    Note basic_ostream<not_char> is not currently supported.
  template <typename ValueSerializer, typename OUTPUT>
  bool serialize(ValueSerializer serializer, OUTPUT* fp) {
    return rep.serialize(serializer, fp);
  }

  // serializer: a functor providing operator()(INPUT*, value_type*)
  //    (reading from INPUT and into value_type).  You can specify a
  //    NopointerSerializer object if appropriate (see above).
  // fp: either a FILE*, OR an istream*/subclass_of_istream*, OR a
  //    pointer to a class providing size_t Read(void*, size_t),
  //    which reads into a buffer from a stream (which fp presumably
  //    owns) and returns the number of bytes successfully read.
  //    Note basic_istream<not_char> is not currently supported.
  // NOTE: Since value_type is std::pair<const Key, T>, ValueSerializer
  // may need to do a const cast in order to fill in the key.
  // NOTE: if Key or T are not POD types, the serializer MUST use
  // placement-new to initialize their values, rather than a normal
  // equals-assignment or similar.  (The value_type* passed into the
  // serializer points to garbage memory.)
  template <typename ValueSerializer, typename INPUT>
  bool unserialize(ValueSerializer serializer, INPUT* fp) {
    return rep.unserialize(serializer, fp);
  }

  // The four methods below are DEPRECATED.
  // Use serialize() and unserialize() for new code.
  template <typename OUTPUT>
  bool write_metadata(OUTPUT *fp)       { return rep.write_metadata(fp); }

  template <typename INPUT>
  bool read_metadata(INPUT *fp)         { return rep.read_metadata(fp); }

  template <typename OUTPUT>
  bool write_nopointer_data(OUTPUT *fp) { return rep.write_nopointer_data(fp); }

  template <typename INPUT>
  bool read_nopointer_data(INPUT *fp)   { return rep.read_nopointer_data(fp); }
};

// We need a global swap as well
template <class Key, class T, class HashFcn, class EqualKey, class Alloc>
inline void swap(sparse_hash_map<Key, T, HashFcn, EqualKey, Alloc>& hm1,
                 sparse_hash_map<Key, T, HashFcn, EqualKey, Alloc>& hm2) {
  hm1.swap(hm2);
}

_END_GOOGLE_NAMESPACE_

#endif /* _SPARSE_HASH_MAP_H_ */
