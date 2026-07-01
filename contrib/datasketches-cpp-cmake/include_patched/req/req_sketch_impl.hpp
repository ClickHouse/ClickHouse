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

#ifndef REQ_SKETCH_IMPL_HPP_
#define REQ_SKETCH_IMPL_HPP_

#include <sstream>
#include <stdexcept>

namespace datasketches {

template<typename T, typename C, typename A>
req_sketch<T, C, A>::req_sketch(uint16_t k, bool hra, const C& comparator, const A& allocator):
comparator_(comparator),
allocator_(allocator),
k_(std::max<uint8_t>(static_cast<int>(k) & -2, static_cast<int>(req_constants::MIN_K))), //rounds down one if odd
hra_(hra),
max_nom_size_(0),
num_retained_(0),
n_(0),
compactors_(allocator),
min_item_(),
max_item_(),
sorted_view_(nullptr)
{
  grow();
}

template<typename T, typename C, typename A>
req_sketch<T, C, A>::~req_sketch() {
  reset_sorted_view();
}

template<typename T, typename C, typename A>
req_sketch<T, C, A>::req_sketch(const req_sketch& other):
comparator_(other.comparator_),
allocator_(other.allocator_),
k_(other.k_),
hra_(other.hra_),
max_nom_size_(other.max_nom_size_),
num_retained_(other.num_retained_),
n_(other.n_),
compactors_(other.compactors_),
min_item_(other.min_item_),
max_item_(other.max_item_),
sorted_view_(nullptr)
{}

template<typename T, typename C, typename A>
req_sketch<T, C, A>::req_sketch(req_sketch&& other) noexcept :
comparator_(std::move(other.comparator_)),
allocator_(std::move(other.allocator_)),
k_(other.k_),
hra_(other.hra_),
max_nom_size_(other.max_nom_size_),
num_retained_(other.num_retained_),
n_(other.n_),
compactors_(std::move(other.compactors_)),
min_item_(std::move(other.min_item_)),
max_item_(std::move(other.max_item_)),
sorted_view_(nullptr)
{}

template<typename T, typename C, typename A>
req_sketch<T, C, A>& req_sketch<T, C, A>::operator=(const req_sketch& other) {
  req_sketch copy(other);
  std::swap(comparator_, copy.comparator_);
  std::swap(allocator_, copy.allocator_);
  std::swap(k_, copy.k_);
  std::swap(hra_, copy.hra_);
  std::swap(max_nom_size_, copy.max_nom_size_);
  std::swap(num_retained_, copy.num_retained_);
  std::swap(n_, copy.n_);
  std::swap(compactors_, copy.compactors_);
  std::swap(min_item_, copy.min_item_);
  std::swap(max_item_, copy.max_item_);
  reset_sorted_view();
  return *this;
}

template<typename T, typename C, typename A>
req_sketch<T, C, A>& req_sketch<T, C, A>::operator=(req_sketch&& other) {
  std::swap(comparator_, other.comparator_);
  std::swap(allocator_, other.allocator_);
  std::swap(k_, other.k_);
  std::swap(hra_, other.hra_);
  std::swap(max_nom_size_, other.max_nom_size_);
  std::swap(num_retained_, other.num_retained_);
  std::swap(n_, other.n_);
  std::swap(compactors_, other.compactors_);
  std::swap(min_item_, other.min_item_);
  std::swap(max_item_, other.max_item_);
  reset_sorted_view();
  return *this;
}

template<typename T, typename C, typename A>
template<typename TT, typename CC, typename AA>
req_sketch<T, C, A>::req_sketch(const req_sketch<TT, CC, AA>& other, const C& comparator, const A& allocator):
comparator_(comparator),
allocator_(allocator),
k_(other.k_),
hra_(other.hra_),
max_nom_size_(other.max_nom_size_),
num_retained_(other.num_retained_),
n_(other.n_),
compactors_(allocator),
min_item_(other.min_item_),
max_item_(other.max_item_),
sorted_view_(nullptr)
{
  static_assert(
    std::is_constructible<T, TT>::value,
    "Type converting constructor requires new type to be constructible from existing type"
  );
  compactors_.reserve(other.compactors_.size());
  for (const auto& compactor: other.compactors_) {
    compactors_.push_back(req_compactor<T, C, A>(compactor, comparator_, allocator_));
  }
}

template<typename T, typename C, typename A>
uint16_t req_sketch<T, C, A>::get_k() const {
  return k_;
}

template<typename T, typename C, typename A>
bool req_sketch<T, C, A>::is_HRA() const {
  return hra_;
}

template<typename T, typename C, typename A>
bool req_sketch<T, C, A>::is_empty() const {
  return n_ == 0;
}

template<typename T, typename C, typename A>
uint64_t req_sketch<T, C, A>::get_n() const {
  return n_;
}

template<typename T, typename C, typename A>
uint32_t req_sketch<T, C, A>::get_num_retained() const {
  return num_retained_;
}

template<typename T, typename C, typename A>
bool req_sketch<T, C, A>::is_estimation_mode() const {
  return compactors_.size() > 1;
}

template<typename T, typename C, typename A>
template<typename FwdT>
void req_sketch<T, C, A>::update(FwdT&& item) {
  if (!check_update_item(item)) { return; }
  if (is_empty()) {
    min_item_.emplace(item);
    max_item_.emplace(item);
  } else {
    if (comparator_(item, *min_item_)) *min_item_ = item;
    if (comparator_(*max_item_, item)) *max_item_ = item;
  }
  compactors_[0].append(std::forward<FwdT>(item));
  ++num_retained_;
  ++n_;
  if (num_retained_ == max_nom_size_) compress();
  reset_sorted_view();
}

template<typename T, typename C, typename A>
template<typename FwdSk>
void req_sketch<T, C, A>::merge(FwdSk&& other) {
  if (is_HRA() != other.is_HRA()) throw std::invalid_argument("merging HRA and LRA is not valid");
  if (other.is_empty()) return;
  if (is_empty()) {
    min_item_.emplace(conditional_forward<FwdSk>(*other.min_item_));
    max_item_.emplace(conditional_forward<FwdSk>(*other.max_item_));
  } else {
    if (comparator_(*other.min_item_, *min_item_)) *min_item_ = conditional_forward<FwdSk>(*other.min_item_);
    if (comparator_(*max_item_, *other.max_item_)) *max_item_ = conditional_forward<FwdSk>(*other.max_item_);
  }
  // grow until this has at least as many compactors as other
  while (get_num_levels() < other.get_num_levels()) grow();
  // merge the items in all height compactors
  for (size_t i = 0; i < other.get_num_levels(); ++i) {
    compactors_[i].merge(conditional_forward<FwdSk>(other.compactors_[i]));
  }
  n_ += other.n_;
  update_max_nom_size();
  update_num_retained();
  if (num_retained_ >= max_nom_size_) compress();
  reset_sorted_view();
}

template<typename T, typename C, typename A>
const T& req_sketch<T, C, A>::get_min_item() const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return *min_item_;
}

template<typename T, typename C, typename A>
const T& req_sketch<T, C, A>::get_max_item() const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return *max_item_;
}

template<typename T, typename C, typename A>
C req_sketch<T, C, A>::get_comparator() const {
  return comparator_;
}

template<typename T, typename C, typename A>
A req_sketch<T, C, A>::get_allocator() const {
  return allocator_;
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::get_rank(const T& item, bool inclusive) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  uint64_t weight = 0;
  for (const auto& compactor: compactors_) {
    weight += compactor.compute_weight(item, inclusive);
  }
  return static_cast<double>(weight) / n_;
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::get_PMF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  setup_sorted_view();
  return sorted_view_->get_PMF(split_points, size, inclusive);
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::get_CDF(const T* split_points, uint32_t size, bool inclusive) const -> vector_double {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  setup_sorted_view();
  return sorted_view_->get_CDF(split_points, size, inclusive);
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::get_quantile(double rank, bool inclusive) const -> quantile_return_type {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  if ((rank < 0.0) || (rank > 1.0)) {
    throw std::invalid_argument("Normalized rank cannot be less than 0 or greater than 1");
  }
  // possible side-effect of sorting level zero
  setup_sorted_view();
  return sorted_view_->get_quantile(rank, inclusive);
}

template<typename T, typename C, typename A>
quantiles_sorted_view<T, C, A> req_sketch<T, C, A>::get_sorted_view() const {
  if (!compactors_[0].is_sorted()) {
    const_cast<Compactor&>(compactors_[0]).sort(); // allow this side effect
  }
  quantiles_sorted_view<T, C, A> view(get_num_retained(), comparator_, allocator_);

  for (auto& compactor: compactors_) {
    view.add(compactor.begin(), compactor.end(), 1ULL << compactor.get_lg_weight());
  }

  view.convert_to_cummulative();
  return view;
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::get_rank_lower_bound(double rank, uint8_t num_std_dev) const {
  return get_rank_lb(get_k(), get_num_levels(), rank, num_std_dev, get_n(), hra_);
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::get_rank_upper_bound(double rank, uint8_t num_std_dev) const {
  return get_rank_ub(get_k(), get_num_levels(), rank, num_std_dev, get_n(), hra_);
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::get_RSE(uint16_t k, double rank, bool hra, uint64_t n) {
  return get_rank_lb(k, 2, rank, 1, n, hra);
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::get_rank_lb(uint16_t k, uint8_t num_levels, double rank, uint8_t num_std_dev, uint64_t n, bool hra) {
  if (is_exact_rank(k, num_levels, rank, n, hra)) return rank;
  const double relative = relative_rse_factor() / k * (hra ? 1.0 - rank : rank);
  const double fixed = FIXED_RSE_FACTOR / k;
  const double lb_rel = rank - num_std_dev * relative;
  const double lb_fix = rank - num_std_dev * fixed;
  return std::max(lb_rel, lb_fix);
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::get_rank_ub(uint16_t k, uint8_t num_levels, double rank, uint8_t num_std_dev, uint64_t n, bool hra) {
  if (is_exact_rank(k, num_levels, rank, n, hra)) return rank;
  const double relative = relative_rse_factor() / k * (hra ? 1.0 - rank : rank);
  const double fixed = FIXED_RSE_FACTOR / k;
  const double ub_rel = rank + num_std_dev * relative;
  const double ub_fix = rank + num_std_dev * fixed;
  return std::min(ub_rel, ub_fix);
}

template<typename T, typename C, typename A>
bool req_sketch<T, C, A>::is_exact_rank(uint16_t k, uint8_t num_levels, double rank, uint64_t n, bool hra) {
  const unsigned base_cap = k * req_constants::INIT_NUM_SECTIONS;
  if (num_levels == 1 || n <= base_cap) return true;
  const double exact_rank_thresh = static_cast<double>(base_cap) / n;
  return (hra && rank >= 1.0 - exact_rank_thresh) || (!hra && rank <= exact_rank_thresh);
}

template<typename T, typename C, typename A>
double req_sketch<T, C, A>::relative_rse_factor() {
  return sqrt(0.0512 / req_constants::INIT_NUM_SECTIONS);
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename C, typename A>
template<typename TT, typename SerDe, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t req_sketch<T, C, A>::get_serialized_size_bytes(const SerDe& sd) const {
  size_t size = PREAMBLE_SIZE_BYTES;
  if (is_empty()) return size;
  if (is_estimation_mode()) {
    size += sizeof(n_) + sizeof(TT) * 2; // min and max
  }
  if (n_ == 1) {
    size += sizeof(TT);
  } else {
    for (const auto& compactor: compactors_) size += compactor.get_serialized_size_bytes(sd);
  }
  return size;
}

// implementation for all other types
template<typename T, typename C, typename A>
template<typename TT, typename SerDe, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t req_sketch<T, C, A>::get_serialized_size_bytes(const SerDe& sd) const {
  size_t size = PREAMBLE_SIZE_BYTES;
  if (is_empty()) return size;
  if (is_estimation_mode()) {
    size += sizeof(n_);
    size += sd.size_of_item(*min_item_);
    size += sd.size_of_item(*max_item_);
  }
  if (n_ == 1) {
    size += sd.size_of_item(*compactors_[0].begin());
  } else {
    for (const auto& compactor: compactors_) size += compactor.get_serialized_size_bytes(sd);
  }
  return size;
}

template<typename T, typename C, typename A>
template<typename SerDe>
void req_sketch<T, C, A>::serialize(std::ostream& os, const SerDe& sd) const {
  const uint8_t preamble_ints = is_estimation_mode() ? 4 : 2;
  write(os, preamble_ints);
  const uint8_t serial_version = SERIAL_VERSION;
  write(os, serial_version);
  const uint8_t family = FAMILY;
  write(os, family);
  const bool raw_items = n_ <= req_constants::MIN_K;
  const uint8_t flags_byte(
      (is_empty() ? 1 << flags::IS_EMPTY : 0)
    | (hra_ ? 1 << flags::IS_HIGH_RANK : 0)
    | (raw_items ? 1 << flags::RAW_ITEMS : 0)
    | (compactors_[0].is_sorted() ? 1 << flags::IS_LEVEL_ZERO_SORTED : 0)
  );
  write(os, flags_byte);
  write(os, k_);
  const uint8_t num_levels = is_empty() ? 0 : get_num_levels();
  write(os, num_levels);
  const uint8_t num_raw_items = raw_items ? static_cast<uint8_t>(n_) : 0;
  write(os, num_raw_items);
  if (is_empty()) return;
  if (is_estimation_mode()) {
    write(os, n_);
    sd.serialize(os, &*min_item_, 1);
    sd.serialize(os, &*max_item_, 1);
  }
  if (raw_items) {
    sd.serialize(os, compactors_[0].begin(), num_raw_items);
  } else {
    for (const auto& compactor: compactors_) compactor.serialize(os, sd);
  }
}

template<typename T, typename C, typename A>
template<typename SerDe>
auto req_sketch<T, C, A>::serialize(unsigned header_size_bytes, const SerDe& sd) const -> vector_bytes {
  const size_t size = header_size_bytes + get_serialized_size_bytes(sd);
  vector_bytes bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  const uint8_t* end_ptr = ptr + size;

  const uint8_t preamble_ints = is_estimation_mode() ? 4 : 2;
  ptr += copy_to_mem(preamble_ints, ptr);
  const uint8_t serial_version = SERIAL_VERSION;
  ptr += copy_to_mem(serial_version, ptr);
  const uint8_t family = FAMILY;
  ptr += copy_to_mem(family, ptr);
  const bool raw_items = n_ <= req_constants::MIN_K;
  const uint8_t flags_byte(
      (is_empty() ? 1 << flags::IS_EMPTY : 0)
    | (hra_ ? 1 << flags::IS_HIGH_RANK : 0)
    | (raw_items ? 1 << flags::RAW_ITEMS : 0)
    | (compactors_[0].is_sorted() ? 1 << flags::IS_LEVEL_ZERO_SORTED : 0)
  );
  ptr += copy_to_mem(flags_byte, ptr);
  ptr += copy_to_mem(k_, ptr);
  const uint8_t num_levels = is_empty() ? 0 : get_num_levels();
  ptr += copy_to_mem(num_levels, ptr);
  const uint8_t num_raw_items = raw_items ? static_cast<uint8_t>(n_) : 0;
  ptr += copy_to_mem(num_raw_items, ptr);
  if (!is_empty()) {
    if (is_estimation_mode()) {
      ptr += copy_to_mem(n_, ptr);
      ptr += sd.serialize(ptr, end_ptr - ptr, &*min_item_, 1);
      ptr += sd.serialize(ptr, end_ptr - ptr, &*max_item_, 1);
    }
    if (raw_items) {
      ptr += sd.serialize(ptr, end_ptr - ptr, compactors_[0].begin(), num_raw_items);
    } else {
      for (const auto& compactor: compactors_) ptr += compactor.serialize(ptr, end_ptr - ptr, sd);
    }
  }
  return bytes;
}

template<typename T, typename C, typename A>
template<typename SerDe>
req_sketch<T, C, A> req_sketch<T, C, A>::deserialize(std::istream& is, const SerDe& sd, const C& comparator, const A& allocator) {
  const auto preamble_ints = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto family_id = read<uint8_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  const auto k = read<uint16_t>(is);
  const auto num_levels = read<uint8_t>(is);
  const auto num_raw_items = read<uint8_t>(is);

  check_preamble_ints(preamble_ints, num_levels);
  check_serial_version(serial_version);
  check_family_id(family_id);

  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  const bool hra = flags_byte & (1 << flags::IS_HIGH_RANK);
  if (is_empty) return req_sketch(k, hra, comparator, allocator);

  optional<T> tmp; // space to deserialize min and max
  tmp.emplace(); // engage so &*tmp is valid under hardened libc++
  optional<T> min_item;
  optional<T> max_item;

  const bool raw_items = flags_byte & (1 << flags::RAW_ITEMS);
  const bool is_level_0_sorted = flags_byte & (1 << flags::IS_LEVEL_ZERO_SORTED);
  std::vector<Compactor, AllocCompactor> compactors(allocator);

  uint64_t n = 1;
  if (num_levels > 1) {
    n = read<uint64_t>(is);
    sd.deserialize(is, &*tmp, 1);
    // serde call did not throw, repackage and cleanup
    min_item.emplace(*tmp);
    (*tmp).~T();
    tmp.emplace(); // re-engage for second use
    sd.deserialize(is, &*tmp, 1);
    // serde call did not throw, repackage and cleanup
    max_item.emplace(*tmp);
    (*tmp).~T();
  }

  if (raw_items) {
    compactors.push_back(Compactor::deserialize(is, sd, comparator, allocator, is_level_0_sorted, k, num_raw_items, hra));
  } else {
    for (size_t i = 0; i < num_levels; ++i) {
      compactors.push_back(Compactor::deserialize(is, sd, comparator, allocator, i == 0 ? is_level_0_sorted : true, hra));
    }
  }
  if (num_levels == 1) {
    const auto begin = compactors[0].begin();
    const auto end = compactors[0].end();
    n = compactors[0].get_num_items();
    auto min_it = begin;
    auto max_it = begin;
    for (auto it = begin; it != end; ++it) {
      if (comparator(*it, *min_it)) min_it = it;
      if (comparator(*max_it, *it)) max_it = it;
    }
    min_item.emplace(*min_it);
    max_item.emplace(*max_it);
  }

  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return req_sketch(k, hra, n, std::move(min_item), std::move(max_item), std::move(compactors), comparator);
}

template<typename T, typename C, typename A>
template<typename SerDe>
req_sketch<T, C, A> req_sketch<T, C, A>::deserialize(const void* bytes, size_t size, const SerDe& sd, const C& comparator, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = static_cast<const char*>(bytes) + size;

  uint8_t preamble_ints;
  ptr += copy_from_mem(ptr, preamble_ints);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags_byte;
  ptr += copy_from_mem(ptr, flags_byte);
  uint16_t k;
  ptr += copy_from_mem(ptr, k);
  uint8_t num_levels;
  ptr += copy_from_mem(ptr, num_levels);
  uint8_t num_raw_items;
  ptr += copy_from_mem(ptr, num_raw_items);

  check_preamble_ints(preamble_ints, num_levels);
  check_serial_version(serial_version);
  check_family_id(family_id);

  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  const bool hra = flags_byte & (1 << flags::IS_HIGH_RANK);
  if (is_empty) return req_sketch(k, hra, comparator, allocator);

  optional<T> tmp; // space to deserialize min and max
  tmp.emplace(); // engage so &*tmp is valid under hardened libc++
  optional<T> min_item;
  optional<T> max_item;

  const bool raw_items = flags_byte & (1 << flags::RAW_ITEMS);
  const bool is_level_0_sorted = flags_byte & (1 << flags::IS_LEVEL_ZERO_SORTED);
  std::vector<Compactor, AllocCompactor> compactors(allocator);

  uint64_t n = 1;
  if (num_levels > 1) {
    ensure_minimum_memory(end_ptr - ptr, sizeof(n));
    ptr += copy_from_mem(ptr, n);
    ptr += sd.deserialize(ptr, end_ptr - ptr, &*tmp, 1);
    // serde call did not throw, repackage and cleanup
    min_item.emplace(*tmp);
    (*tmp).~T();
    tmp.emplace(); // re-engage for second use
    ptr += sd.deserialize(ptr, end_ptr - ptr, &*tmp, 1);
    // serde call did not throw, repackage and cleanup
    max_item.emplace(*tmp);
    (*tmp).~T();
  }

  if (raw_items) {
    auto pair = Compactor::deserialize(ptr, end_ptr - ptr, sd, comparator, allocator, is_level_0_sorted, k, num_raw_items, hra);
    compactors.push_back(std::move(pair.first));
    ptr += pair.second;
  } else {
    for (size_t i = 0; i < num_levels; ++i) {
      auto pair = Compactor::deserialize(ptr, end_ptr - ptr, sd, comparator, allocator, i == 0 ? is_level_0_sorted : true, hra);
      compactors.push_back(std::move(pair.first));
      ptr += pair.second;
    }
  }
  if (num_levels == 1) {
    const auto begin = compactors[0].begin();
    const auto end = compactors[0].end();
    n = compactors[0].get_num_items();
    auto min_it = begin;
    auto max_it = begin;
    for (auto it = begin; it != end; ++it) {
      if (comparator(*it, *min_it)) min_it = it;
      if (comparator(*max_it, *it)) max_it = it;
    }
    min_item.emplace(*min_it);
    max_item.emplace(*max_it);
  }

  return req_sketch(k, hra, n, std::move(min_item), std::move(max_item), std::move(compactors), comparator);
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::grow() {
  const uint8_t lg_weight = get_num_levels();
  compactors_.push_back(Compactor(hra_, lg_weight, k_, comparator_, allocator_));
  update_max_nom_size();
}

template<typename T, typename C, typename A>
uint8_t req_sketch<T, C, A>::get_num_levels() const {
  return static_cast<uint8_t>(compactors_.size());
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::update_max_nom_size() {
  max_nom_size_ = 0;
  for (const auto& compactor: compactors_) max_nom_size_ += compactor.get_nom_capacity();
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::update_num_retained() {
  num_retained_ = 0;
  for (const auto& compactor: compactors_) num_retained_ += compactor.get_num_items();
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::compress() {
  for (size_t h = 0; h < compactors_.size(); ++h) {
    if (compactors_[h].get_num_items() >= compactors_[h].get_nom_capacity()) {
      if (h == 0) compactors_[0].sort();
      if (h + 1 >= get_num_levels()) { // at the top?
        grow(); // add a level, increases max_nom_size
      }
      auto pair = compactors_[h].compact(compactors_[h + 1]);
      num_retained_ -= pair.first;
      max_nom_size_ += pair.second;
      if (LAZY_COMPRESSION && num_retained_ < max_nom_size_) break;
    }
  }
}

template<typename T, typename C, typename A>
string<A> req_sketch<T, C, A>::to_string(bool print_levels, bool print_items) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### REQ sketch summary:" << std::endl;
  os << "   K              : " << k_ << std::endl;
  os << "   High Rank Acc  : " << (hra_ ? "true" : "false") << std::endl;
  os << "   Empty          : " << (is_empty() ? "true" : "false") << std::endl;
  os << "   Estimation mode: " << (is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   Sorted         : " << (compactors_[0].is_sorted() ? "true" : "false") << std::endl;
  os << "   N              : " << n_ << std::endl;
  os << "   Levels         : " << compactors_.size() << std::endl;
  os << "   Retained items : " << num_retained_ << std::endl;
  os << "   Capacity items : " << max_nom_size_ << std::endl;
  if (!is_empty()) {
    os << "   Min item      : " << *min_item_ << std::endl;
    os << "   Max item      : " << *max_item_ << std::endl;
  }
  os << "### End sketch summary" << std::endl;

  if (print_levels) {
    os << "### REQ sketch levels:" << std::endl;
    os << "   index: nominal capacity, actual size" << std::endl;
    for (uint8_t i = 0; i < compactors_.size(); i++) {
      os << "   " << (unsigned int) i << ": "
        << compactors_[i].get_nom_capacity() << ", "
        << compactors_[i].get_num_items() << std::endl;
    }
    os << "### End sketch levels" << std::endl;
  }

  if (print_items) {
    os << "### REQ sketch data:" << std::endl;
    unsigned level = 0;
    for (const auto& compactor: compactors_) {
      os << " level " << level << ": " << std::endl;
      for (auto it = compactor.begin(); it != compactor.end(); ++it) {
        os << "   " << *it << std::endl;
      }
      ++level;
    }
    os << "### End sketch data" << std::endl;
  }
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename C, typename A>
req_sketch<T, C, A>::req_sketch(uint16_t k, bool hra, uint64_t n,
    optional<T>&& min_item, optional<T>&& max_item,
    std::vector<Compactor, AllocCompactor>&& compactors, const C& comparator):
comparator_(comparator),
allocator_(compactors.get_allocator()),
k_(k),
hra_(hra),
max_nom_size_(0),
num_retained_(0),
n_(n),
compactors_(std::move(compactors)),
min_item_(std::move(min_item)),
max_item_(std::move(max_item)),
sorted_view_(nullptr)
{
  update_max_nom_size();
  update_num_retained();
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::check_preamble_ints(uint8_t preamble_ints, uint8_t num_levels) {
  const uint8_t expected_preamble_ints = num_levels > 1 ? 4 : 2;
  if (preamble_ints != expected_preamble_ints) {
    throw std::invalid_argument("Possible corruption: preamble ints must be "
        + std::to_string(expected_preamble_ints) + ", got " + std::to_string(preamble_ints));
  }
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::check_serial_version(uint8_t serial_version) {
  if (serial_version != SERIAL_VERSION) {
    throw std::invalid_argument("Possible corruption: serial version mismatch: expected "
        + std::to_string(SERIAL_VERSION)
        + ", got " + std::to_string(serial_version));
  }
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::check_family_id(uint8_t family_id) {
  if (family_id != FAMILY) {
    throw std::invalid_argument("Possible corruption: family mismatch: expected "
        + std::to_string(FAMILY) + ", got " + std::to_string(family_id));
  }
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::begin() const -> const_iterator {
  return const_iterator(compactors_.begin(), compactors_.end());
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::end() const -> const_iterator {
  return const_iterator(compactors_.end(), compactors_.end());
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::setup_sorted_view() const {
  if (sorted_view_ == nullptr) {
    using AllocSortedView = typename std::allocator_traits<A>::template rebind_alloc<quantiles_sorted_view<T, C, A>>;
    sorted_view_ = new (AllocSortedView(allocator_).allocate(1)) quantiles_sorted_view<T, C, A>(get_sorted_view());
  }
}

template<typename T, typename C, typename A>
void req_sketch<T, C, A>::reset_sorted_view() {
  if (sorted_view_ != nullptr) {
    sorted_view_->~quantiles_sorted_view();
    using AllocSortedView = typename std::allocator_traits<A>::template rebind_alloc<quantiles_sorted_view<T, C, A>>;
    AllocSortedView(allocator_).deallocate(sorted_view_, 1);
    sorted_view_ = nullptr;
  }
}

// iterator

template<typename T, typename C, typename A>
req_sketch<T, C, A>::const_iterator::const_iterator(LevelsIterator begin, LevelsIterator end):
    levels_it_(begin),
    levels_end_(end),
    compactor_it_(begin == end ? nullptr : (*levels_it_).begin())
{}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::const_iterator::operator++() -> const_iterator& {
  ++compactor_it_;
  if (compactor_it_ == (*levels_it_).end()) {
    ++levels_it_;
    if (levels_it_ != levels_end_) compactor_it_ = (*levels_it_).begin();
  }
  return *this;
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::const_iterator::operator++(int) -> const_iterator& {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename C, typename A>
bool req_sketch<T, C, A>::const_iterator::operator==(const const_iterator& other) const {
  if (levels_it_ != other.levels_it_) return false;
  if (levels_it_ == levels_end_) return true;
  return compactor_it_ == other.compactor_it_;
}

template<typename T, typename C, typename A>
bool req_sketch<T, C, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::const_iterator::operator*() const -> reference {
  return value_type(*compactor_it_, 1ULL << (*levels_it_).get_lg_weight());
}

template<typename T, typename C, typename A>
auto req_sketch<T, C, A>::const_iterator::operator->() const -> pointer {
  return **this;
}

} /* namespace datasketches */

#endif
