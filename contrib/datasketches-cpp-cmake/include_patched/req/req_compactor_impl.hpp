/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef REQ_COMPACTOR_IMPL_HPP_
#define REQ_COMPACTOR_IMPL_HPP_

#include <stdexcept>
#include <cmath>
#include <algorithm>

#include "count_zeros.hpp"
#include "conditional_forward.hpp"
#include "common_defs.hpp"

namespace datasketches {

template<typename T, typename C, typename A>
req_compactor<T, C, A>::req_compactor(bool hra, uint8_t lg_weight, uint32_t section_size,
    const C& comparator, const A& allocator, bool sorted):
comparator_(comparator),
allocator_(allocator),
lg_weight_(lg_weight),
hra_(hra),
coin_(false),
sorted_(sorted),
section_size_raw_(static_cast<float>(section_size)),
section_size_(section_size),
num_sections_(req_constants::INIT_NUM_SECTIONS),
state_(0),
num_items_(0),
capacity_(2 * get_nom_capacity()),
items_(allocator_.allocate(capacity_))
{}

template<typename T, typename C, typename A>
req_compactor<T, C, A>::~req_compactor() {
  if (items_ != nullptr) {
    for (auto it = begin(); it != end(); ++it) (*it).~T();
    allocator_.deallocate(items_, capacity_);
  }
}

template<typename T, typename C, typename A>
req_compactor<T, C, A>::req_compactor(const req_compactor& other):
comparator_(other.comparator_),
allocator_(other.allocator_),
lg_weight_(other.lg_weight_),
hra_(other.hra_),
coin_(other.coin_),
sorted_(other.sorted_),
section_size_raw_(other.section_size_raw_),
section_size_(other.section_size_),
num_sections_(other.num_sections_),
state_(other.state_),
num_items_(other.num_items_),
capacity_(other.capacity_),
items_(nullptr)
{
  if (other.items_ != nullptr) {
    items_ = allocator_.allocate(capacity_);
    const uint32_t from = hra_ ? capacity_ - num_items_ : 0;
    const uint32_t to = hra_ ? capacity_ : num_items_;
    for (uint32_t i = from; i < to; ++i) new (items_ + i) T(other.items_[i]);
  }
}

template<typename T, typename C, typename A>
req_compactor<T, C, A>::req_compactor(req_compactor&& other) noexcept :
comparator_(std::move(other.comparator_)),
allocator_(std::move(other.allocator_)),
lg_weight_(other.lg_weight_),
hra_(other.hra_),
coin_(other.coin_),
sorted_(other.sorted_),
section_size_raw_(other.section_size_raw_),
section_size_(other.section_size_),
num_sections_(other.num_sections_),
state_(other.state_),
num_items_(other.num_items_),
capacity_(other.capacity_),
items_(other.items_)
{
  other.items_ = nullptr;
}

template<typename T, typename C, typename A>
req_compactor<T, C, A>& req_compactor<T, C, A>::operator=(const req_compactor& other) {
  req_compactor copy(other);
  std::swap(comparator_, copy.comparator_);
  std::swap(allocator_, copy.allocator_);
  std::swap(lg_weight_, copy.lg_weight_);
  std::swap(hra_, copy.hra_);
  std::swap(coin_, copy.coin_);
  std::swap(sorted_, copy.sorted_);
  std::swap(section_size_raw_, copy.section_size_raw_);
  std::swap(section_size_, copy.section_size_);
  std::swap(num_sections_, copy.num_sections_);
  std::swap(state_, copy.state_);
  std::swap(num_items_, copy.num_items_);
  std::swap(capacity_, copy.capacity_);
  std::swap(items_, copy.items_);
  return *this;
}

template<typename T, typename C, typename A>
req_compactor<T, C, A>& req_compactor<T, C, A>::operator=(req_compactor&& other) {
  std::swap(comparator_, other.comparator_);
  std::swap(allocator_, other.allocator_);
  std::swap(lg_weight_, other.lg_weight_);
  std::swap(hra_, other.hra_);
  std::swap(coin_, other.coin_);
  std::swap(sorted_, other.sorted_);
  std::swap(section_size_raw_, other.section_size_raw_);
  std::swap(section_size_, other.section_size_);
  std::swap(num_sections_, other.num_sections_);
  std::swap(state_, other.state_);
  std::swap(num_items_, other.num_items_);
  std::swap(capacity_, other.capacity_);
  std::swap(items_, other.items_);
  return *this;
}

template<typename T, typename C, typename A>
template<typename TT, typename CC, typename AA>
req_compactor<T, C, A>::req_compactor(const req_compactor<TT, CC, AA>& other, const C& comparator, const A& allocator):
comparator_(comparator),
allocator_(allocator),
lg_weight_(other.lg_weight_),
hra_(other.hra_),
coin_(other.coin_),
sorted_(other.sorted_),
section_size_raw_(other.section_size_raw_),
section_size_(other.section_size_),
num_sections_(other.num_sections_),
state_(other.state_),
num_items_(other.num_items_),
capacity_(other.capacity_),
items_(nullptr)
{
  if (other.items_ != nullptr) {
    items_ = allocator_.allocate(capacity_);
    const uint32_t from = hra_ ? capacity_ - num_items_ : 0;
    const uint32_t to = hra_ ? capacity_ : num_items_;
    for (uint32_t i = from; i < to; ++i) new (items_ + i) T(other.items_[i]);
    if (sorted_ && !std::is_sorted(items_ + from, items_ + to, comparator_)) {
      throw std::logic_error("items must be sorted");
    }
  }
}

template<typename T, typename C, typename A>
bool req_compactor<T, C, A>::is_sorted() const {
  return sorted_;
}

template<typename T, typename C, typename A>
uint32_t req_compactor<T, C, A>::get_num_items() const {
  return num_items_;
}

template<typename T, typename C, typename A>
uint32_t req_compactor<T, C, A>::get_nom_capacity() const {
  return req_constants::MULTIPLIER * num_sections_ * section_size_;
}

template<typename T, typename C, typename A>
uint8_t req_compactor<T, C, A>::get_lg_weight() const {
  return lg_weight_;
}

template<typename T, typename C, typename A>
uint64_t req_compactor<T, C, A>::compute_weight(const T& item, bool inclusive) const {
  if (!sorted_) const_cast<req_compactor*>(this)->sort(); // allow sorting as a side effect
  auto it = inclusive ?
      std::upper_bound(begin(), end(), item, comparator_) :
      std::lower_bound(begin(), end(), item, comparator_);
  return std::distance(begin(), it) << lg_weight_;
}

template<typename T, typename C, typename A>
template<typename FwdT>
void req_compactor<T, C, A>::append(FwdT&& item) {
  if (num_items_ == capacity_) grow(capacity_ + get_nom_capacity());
  const uint32_t i = hra_ ? capacity_ - num_items_ - 1 : num_items_;
  new (items_ + i) T(std::forward<FwdT>(item));
  ++num_items_;
  if (num_items_ > 1) sorted_ = false;
}

template<typename T, typename C, typename A>
void req_compactor<T, C, A>::grow(uint32_t new_capacity) {
  T* new_items = allocator_.allocate(new_capacity);
  uint32_t new_i = hra_ ? new_capacity - num_items_ : 0;
  for (auto it = begin(); it != end(); ++it, ++new_i) {
    new (new_items + new_i) T(std::move(*it));
    (*it).~T();
  }
  allocator_.deallocate(items_, capacity_);
  items_ = new_items;
  capacity_ = new_capacity;
}

template<typename T, typename C, typename A>
void req_compactor<T, C, A>::ensure_space(uint32_t num) {
  if (num_items_ + num > capacity_) grow(num_items_ + num + get_nom_capacity());
}

template<typename T, typename C, typename A>
const T* req_compactor<T, C, A>::begin() const {
  return items_ + (hra_ ? capacity_ - num_items_ : 0);
}

template<typename T, typename C, typename A>
const T* req_compactor<T, C, A>::end() const {
  return items_ + (hra_ ? capacity_ : num_items_);
}

template<typename T, typename C, typename A>
T* req_compactor<T, C, A>::begin() {
  return items_ + (hra_ ? capacity_ - num_items_ : 0);
}

template<typename T, typename C, typename A>
T* req_compactor<T, C, A>::end() {
  return items_ + (hra_ ? capacity_ : num_items_);
}

template<typename T, typename C, typename A>
template<typename FwdC>
void req_compactor<T, C, A>::merge(FwdC&& other) {
  // TODO: swap if other is larger?
  if (lg_weight_ != other.lg_weight_) throw std::logic_error("weight mismatch");
  state_ |= other.state_;
  while (ensure_enough_sections()) {}
  ensure_space(other.get_num_items());
  sort();
  auto offset = hra_ ? capacity_ - num_items_ : num_items_;
  auto from = hra_ ? begin() - other.get_num_items() : end();
  auto to = from + other.get_num_items();
  auto other_it = other.begin();
  for (auto it = from; it != to; ++it, ++other_it) new (it) T(conditional_forward<FwdC>(*other_it));
  if (!other.sorted_) std::sort(from, to, comparator_);
  if (num_items_ > 0) std::inplace_merge(hra_ ? from : begin(), items_ + offset, hra_ ? end() : to, C());
  num_items_ += other.get_num_items();
}

template<typename T, typename C, typename A>
void req_compactor<T, C, A>::sort() {
  if (!sorted_) {
    std::sort(begin(), end(), comparator_);
    sorted_ = true;
  }
}

template<typename T, typename C, typename A>
std::pair<uint32_t, uint32_t> req_compactor<T, C, A>::compact(req_compactor& next) {
  const uint32_t starting_nom_capacity = get_nom_capacity();
  // choose a part of the buffer to compact
  const uint32_t secs_to_compact = std::min<uint32_t>(count_trailing_zeros_in_u64(~state_) + 1, num_sections_);
  auto compaction_range = compute_compaction_range(secs_to_compact);
  if (compaction_range.second - compaction_range.first < 2) throw std::logic_error("compaction range error");

  if ((state_ & 1) == 1) { coin_ = !coin_; } // for odd flip coin;
  else { coin_ = random_utils::random_bit(); } // random coin flip

  const auto num = (compaction_range.second - compaction_range.first) / 2;
  next.ensure_space(num);
  auto next_middle = hra_ ? next.begin() : next.end();
  auto next_empty = hra_ ? next.begin() - num : next.end();
  promote_evens_or_odds(begin() + compaction_range.first, begin() + compaction_range.second, coin_, next_empty);
  next.num_items_ += num;
  std::inplace_merge(next.begin(), next_middle, next.end(), comparator_);
  for (size_t i = compaction_range.first; i < compaction_range.second; ++i) (*(begin() + i)).~T();
  num_items_ -= compaction_range.second - compaction_range.first;

  ++state_;
  ensure_enough_sections();
  return std::pair<uint32_t, uint32_t>(
    num,
    get_nom_capacity() - starting_nom_capacity
  );
}

template<typename T, typename C, typename A>
bool req_compactor<T, C, A>::ensure_enough_sections() {
  const float ssr = section_size_raw_ / sqrtf(2);
  const uint32_t ne = nearest_even(ssr);
  if (state_ >= static_cast<uint64_t>(1ULL << (num_sections_ - 1)) && ne >= req_constants::MIN_K) {
    section_size_raw_ = ssr;
    section_size_ = ne;
    num_sections_ <<= 1;
    if (capacity_ < 2 * get_nom_capacity()) grow(2 * get_nom_capacity());
    return true;
  }
  return false;
}

template<typename T, typename C, typename A>
std::pair<uint32_t, uint32_t> req_compactor<T, C, A>::compute_compaction_range(uint32_t secs_to_compact) const {
  uint32_t non_compact = get_nom_capacity() / 2 + (num_sections_ - secs_to_compact) * section_size_;
  // make compacted region even
  if (((num_items_ - non_compact) & 1) == 1) ++non_compact;
  const uint32_t low = hra_ ? 0 : non_compact;
  const uint32_t high = hra_ ? num_items_ - non_compact : num_items_;
  return std::pair<uint32_t, uint32_t>(low, high);
}

template<typename T, typename C, typename A>
uint32_t req_compactor<T, C, A>::nearest_even(float value) {
  return static_cast<uint32_t>(round(value / 2)) << 1;
}

template<typename T, typename C, typename A>
template<typename InIter, typename OutIter>
void req_compactor<T, C, A>::promote_evens_or_odds(InIter from, InIter to, bool odds, OutIter dst) {
  if (from == to) return;
  InIter i = from;
  if (odds) ++i;
  while (i != to) {
    new (dst) T(std::move(*i));
    ++dst;
    ++i;
    if (i == to) break;
    ++i;
  }
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename C, typename A>
template<typename S, typename TT, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t req_compactor<T, C, A>::get_serialized_size_bytes(const S&) const {
  return sizeof(state_) + sizeof(section_size_raw_) + sizeof(lg_weight_) + sizeof(num_sections_) +
      sizeof(uint16_t) + // padding
      sizeof(uint32_t) + // num_items
      sizeof(TT) * num_items_;
}

// implementation for all other types
template<typename T, typename C, typename A>
template<typename S, typename TT, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t req_compactor<T, C, A>::get_serialized_size_bytes(const S& serde) const {
  size_t size = sizeof(state_) + sizeof(section_size_raw_) + sizeof(lg_weight_) + sizeof(num_sections_) +
      sizeof(uint16_t) + // padding
      sizeof(uint32_t); // num_items
  for (auto it = begin(); it != end(); ++it) size += serde.size_of_item(*it);
  return size;
}

template<typename T, typename C, typename A>
template<typename S>
void req_compactor<T, C, A>::serialize(std::ostream& os, const S& serde) const {
  write(os, state_);
  write(os, section_size_raw_);
  write(os, lg_weight_);
  write(os, num_sections_);
  const uint16_t padding = 0;
  write(os, padding);
  write(os, num_items_);
  serde.serialize(os, begin(), num_items_);
}

template<typename T, typename C, typename A>
template<typename S>
size_t req_compactor<T, C, A>::serialize(void* dst, size_t capacity, const S& serde) const {
  uint8_t* ptr = static_cast<uint8_t*>(dst);
  const uint8_t* end_ptr = ptr + capacity;
  ptr += copy_to_mem(state_, ptr);
  ptr += copy_to_mem(section_size_raw_, ptr);
  ptr += copy_to_mem(lg_weight_, ptr);
  ptr += copy_to_mem(num_sections_, ptr);
  const uint16_t padding = 0;
  ptr += copy_to_mem(padding, ptr);
  ptr += copy_to_mem(num_items_, ptr);
  ptr += serde.serialize(ptr, end_ptr - ptr, begin(), num_items_);
  return ptr - static_cast<uint8_t*>(dst);
}

template<typename T, typename C, typename A>
template<typename S>
req_compactor<T, C, A> req_compactor<T, C, A>::deserialize(std::istream& is, const S& serde,
    const C& comparator, const A& allocator, bool sorted, bool hra) {
  auto state = read<decltype(state_)>(is);
  auto section_size_raw = read<decltype(section_size_raw_)>(is);
  auto lg_weight = read<decltype(lg_weight_)>(is);
  auto num_sections = read<decltype(num_sections_)>(is);
  read<uint16_t>(is); // padding
  auto num_items = read<uint32_t>(is);
  auto items = deserialize_items(is, serde, allocator, num_items);
  return req_compactor(hra, lg_weight, sorted, section_size_raw, num_sections, state, std::move(items), num_items,
      comparator, allocator);
}

template<typename T, typename C, typename A>
template<typename S>
req_compactor<T, C, A> req_compactor<T, C, A>::deserialize(std::istream& is, const S& serde,
    const C& comparator, const A& allocator, bool sorted, uint16_t k, uint8_t num_items, bool hra) {
  auto items = deserialize_items(is, serde, allocator, num_items);
  return req_compactor(hra, 0, sorted, k, req_constants::INIT_NUM_SECTIONS, 0, std::move(items), num_items,
      comparator, allocator);
}

template<typename T, typename C, typename A>
template<typename S>
auto req_compactor<T, C, A>::deserialize_items(std::istream& is, const S& serde, const A& allocator, uint32_t num)
-> std::unique_ptr<T, items_deleter> {
  A alloc(allocator);
  std::unique_ptr<T, items_deleter> items(alloc.allocate(num), items_deleter(allocator, false, num));
  serde.deserialize(is, items.get(), num);
  // serde did not throw, enable destructors
  items.get_deleter().set_destroy(true);
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return items;
}

template<typename T, typename C, typename A>
template<typename S>
std::pair<req_compactor<T, C, A>, size_t> req_compactor<T, C, A>::deserialize(const void* bytes, size_t size,
    const S& serde, const C& comparator, const A& allocator, bool sorted, bool hra) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = static_cast<const char*>(bytes) + size;

  uint64_t state;
  ptr += copy_from_mem(ptr, state);
  float section_size_raw;
  ptr += copy_from_mem(ptr, section_size_raw);
  uint8_t lg_weight;
  ptr += copy_from_mem(ptr, lg_weight);
  uint8_t num_sections;
  ptr += copy_from_mem(ptr, num_sections);
  ptr += 2; // padding
  uint32_t num_items;
  ptr += copy_from_mem(ptr, num_items);
  auto pair = deserialize_items(ptr, end_ptr - ptr, serde, allocator, num_items);
  ptr += pair.second;
  return std::pair<req_compactor, size_t>(
      req_compactor(hra, lg_weight, sorted, section_size_raw, num_sections, state, std::move(pair.first), num_items,
          comparator, allocator),
      ptr - static_cast<const char*>(bytes)
  );
}

template<typename T, typename C, typename A>
template<typename S>
std::pair<req_compactor<T, C, A>, size_t> req_compactor<T, C, A>::deserialize(const void* bytes, size_t size,
    const S& serde, const C& comparator, const A& allocator, bool sorted, uint16_t k, uint8_t num_items, bool hra) {
  auto pair = deserialize_items(bytes, size, serde, allocator, num_items);
  return std::pair<req_compactor, size_t>(
      req_compactor(hra, 0, sorted, k, req_constants::INIT_NUM_SECTIONS, 0, std::move(pair.first), num_items,
          comparator, allocator),
      pair.second
  );
}

template<typename T, typename C, typename A>
template<typename S>
auto req_compactor<T, C, A>::deserialize_items(const void* bytes, size_t size, const S& serde, const A& allocator, uint32_t num)
-> std::pair<std::unique_ptr<T, items_deleter>, size_t> {
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = static_cast<const char*>(bytes) + size;
  A alloc(allocator);
  std::unique_ptr<T, items_deleter> items(alloc.allocate(num), items_deleter(allocator, false, num));
  ptr += serde.deserialize(ptr, end_ptr - ptr, items.get(), num);
  // serde did not throw, enable destructors
  items.get_deleter().set_destroy(true);
  return std::pair<std::unique_ptr<T, items_deleter>, size_t>(
      std::move(items),
      ptr - static_cast<const char*>(bytes)
  );
}


template<typename T, typename C, typename A>
req_compactor<T, C, A>::req_compactor(bool hra, uint8_t lg_weight, bool sorted, float section_size_raw, uint8_t num_sections,
    uint64_t state, std::unique_ptr<T, items_deleter> items, uint32_t num_items, const C& comparator, const A& allocator):
comparator_(comparator),
allocator_(allocator),
lg_weight_(lg_weight),
hra_(hra),
coin_(random_utils::random_bit()),
sorted_(sorted),
section_size_raw_(section_size_raw),
section_size_(nearest_even(section_size_raw)),
num_sections_(num_sections),
state_(state),
num_items_(num_items),
capacity_(num_items),
items_(items.release())
{}

template<typename T, typename C, typename A>
class req_compactor<T, C, A>::items_deleter {
  public:
  items_deleter(const A& allocator, bool destroy, size_t num): allocator_(allocator), destroy_(destroy), num_(num) {}
  void operator() (T* ptr) {
    if (ptr != nullptr) {
      if (destroy_) {
        for (size_t i = 0; i < num_; ++i) {
          ptr[i].~T();
        }
      }
      allocator_.deallocate(ptr, num_);
    }
  }
  void set_destroy(bool destroy) { destroy_ = destroy; }
  private:
  A allocator_;
  bool destroy_;
  size_t num_;
};

} /* namespace datasketches */

#endif
