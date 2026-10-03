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

#ifndef REQ_SKETCH_HPP_
#define REQ_SKETCH_HPP_

#include <iterator>

#include "req_common.hpp"
#include "req_compactor.hpp"
#include "quantiles_sorted_view.hpp"
#include "optional.hpp"

namespace datasketches {

/**
 * Relative Error Quantiles Sketch.
 * This is an implementation based on the paper
 * "Relative Error Streaming Quantiles" by Graham Cormode, Zohar Karnin, Edo Liberty,
 * Justin Thaler, Pavel Veselý, and loosely derived from a Python prototype written by Pavel Veselý.
 *
 * <p>Reference: https://arxiv.org/abs/2004.01668</p>
 *
 * <p>This implementation differs from the algorithm described in the paper in the following:</p>
 *
 * <ul>
 * <li>The algorithm requires no upper bound on the stream length.
 * Instead, each relative-compactor counts the number of compaction operations performed
 * so far (via variable state). Initially, the relative-compactor starts with INIT_NUMBER_OF_SECTIONS.
 * Each time the number of compactions (variable state) exceeds 2^{numSections - 1}, we double
 * numSections. Note that after merging the sketch with another one variable state may not correspond
 * to the number of compactions performed at a particular level, however, since the state variable
 * never exceeds the number of compactions, the guarantees of the sketch remain valid.</li>
 *
 * <li>The size of each section (variable k and section_size in the code and parameter k in
 * the paper) is initialized with a number set by the user via variable k.
 * When the number of sections doubles, we decrease section_size by a factor of sqrt(2).
 * This is applied at each level separately. Thus, when we double the number of sections, the
 * nominal compactor size increases by a factor of approx. sqrt(2) (+/- rounding).</li>
 *
 * <li>The merge operation here does not perform "special compactions", which are used in the paper
 * to allow for a tight mathematical analysis of the sketch.</li>
 * </ul>
 *
 * <p>This implementation provides a number of capabilities not discussed in the paper or provided
 * in the Python prototype.</p>
 *
 * <ul><li>The Python prototype only implemented high accuracy for low ranks. This implementation
 * provides the user with the ability to choose either high rank accuracy or low rank accuracy at
 * the time of sketch construction.</li>
 * <li>The Python prototype only implemented a comparison criterion of "INCLUSIVE". This implementation
 * allows the user to use both the "INCLUSIVE" criterion and the "EXCLUSIVE" criterion.</li>
 * <li>This implementation provides extensive debug visibility into the operation of the sketch with
 * two levels of detail output. This is not only useful for debugging, but is a powerful tool to
 * help users understand how the sketch works.</li>
 * </ul>
 */
template<
  typename T,
  typename Comparator = std::less<T>, // strict weak ordering function (see C++ named requirements: Compare)
  typename Allocator = std::allocator<T>
>
class req_sketch {
public:
  using value_type = T;
  using comparator = Comparator;
  using allocator_type = Allocator;
  using Compactor = req_compactor<T, Comparator, Allocator>;
  using AllocCompactor = typename std::allocator_traits<Allocator>::template rebind_alloc<Compactor>;
  using vector_double = typename quantiles_sorted_view<T, Comparator, Allocator>::vector_double;

  /**
   * Quantile return type.
   * This is to return quantiles either by value (for arithmetic types) or by const reference (for all other types)
   */
  using quantile_return_type = typename quantiles_sorted_view<T, Comparator, Allocator>::quantile_return_type;

  /**
   * Constructor
   * @param k Controls the size and error of the sketch. It must be even and in the range [4, 1024], inclusive.
   * Value of 12 roughly corresponds to 1% relative error guarantee at 95% confidence.
   * @param hra if true, the default, the high ranks are prioritized for better
   * accuracy. Otherwise the low ranks are prioritized for better accuracy.
   * @param comparator strict weak ordering function (see C++ named requirements: Compare)
   * @param allocator used by this sketch to allocate memory
   */
  explicit req_sketch(uint16_t k, bool hra = true, const Comparator& comparator = Comparator(),
      const Allocator& allocator = Allocator());

  /**
   * Copy constructor
   * @param other sketch to be copied
   */
  req_sketch(const req_sketch& other);

  /**
   * Move constructor
   * @param other sketch to be moved
   */
  req_sketch(req_sketch&& other) noexcept;

  ~req_sketch();

  /**
   * Copy assignment
   * @param other sketch to be copied
   * @return reference to this sketch
   */
  req_sketch& operator=(const req_sketch& other);

  /**
   * Move assignment
   * @param other sketch to be moved
   * @return reference to this sketch
   */
  req_sketch& operator=(req_sketch&& other);

  /**
   * Type converting constructor.
   * @param other sketch of a different type
   * @param comparator instance of a Comparator
   * @param allocator instance of an Allocator
   */
  template<typename TT, typename CC, typename AA>
  explicit req_sketch(const req_sketch<TT, CC, AA>& other, const Comparator& comparator = Comparator(),
      const Allocator& allocator = Allocator());

  /**
   * Returns configured parameter K
   * @return parameter K
   */
  uint16_t get_k() const;

  /**
   * Returns configured parameter High Rank Accuracy
   * @return parameter HRA
   */
  bool is_HRA() const;

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  uint64_t get_n() const;

  /**
   * Returns the number of retained items in the sketch.
   * @return number of retained items
   */
  uint32_t get_num_retained() const;

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  bool is_estimation_mode() const;

  /**
   * Updates this sketch with the given data item.
   * @param item from a stream of items
   */
  template<typename FwdT>
  void update(FwdT&& item);

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  template<typename FwdSk>
  void merge(FwdSk&& other);

  /**
   * Returns the min item of the stream.
   * If the sketch is empty this throws std::runtime_error.
   * @return the min item of the stream
   */
  const T& get_min_item() const;

  /**
   * Returns the max item of the stream.
   * If the sketch is empty this throws std::runtime_error.
   * @return the max item of the stream
   */
  const T& get_max_item() const;

  /**
   * Returns an instance of the comparator for this sketch.
   * @return comparator
   */
  Comparator get_comparator() const;

  /**
   * Returns an instance of the allocator for this sketch.
   * @return allocator
   */
  Allocator get_allocator() const;

  /**
   * Returns an approximation to the normalized rank of the given item from 0 to 1 inclusive.
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param item to be ranked.
   * @param inclusive if true the weight of the given item is included into the rank.
   * Otherwise the rank equals the sum of the weights of all items that are less than the given item
   * according to the comparator C.
   *
   * @return an approximate rank of the given item
   */
  double get_rank(const T& item, bool inclusive = true) const;

  /**
   * Returns an approximation to the Probability Mass Function (PMF) of the input stream
   * given a set of split points (items).
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing items
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals (bins).
   *
   * @param size the number of split points in the array
   *
   * @param inclusive if true the rank of an item includes its own weight, and therefore
   * if the sketch contains items equal to a slit point, then in PMF such items are
   * included into the interval to the left of split point. Otherwise they are included into the interval
   * to the right of split point.
   *
   * @return an array of m+1 doubles each of which is an approximation
   * to the fraction of the input stream items (the mass) that fall into one of those intervals.
   */
  vector_double get_PMF(const T* split_points, uint32_t size, bool inclusive = true) const;

  /**
   * Returns an approximation to the Cumulative Distribution Function (CDF), which is the
   * cumulative analog of the PMF, of the input stream given a set of split points (items).
   *
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param split_points an array of <i>m</i> unique, monotonically increasing items
   * that divide the input domain into <i>m+1</i> consecutive disjoint intervals.
   *
   * @param size the number of split points in the array
   *
   * @param inclusive if true the rank of an item includes its own weight, and therefore
   * if the sketch contains items equal to a slit point, then in CDF such items are
   * included into the interval to the left of split point. Otherwise they are included into
   * the interval to the right of split point.
   *
   * @return an array of m+1 doubles, which are a consecutive approximation to the CDF
   * of the input stream given the split_points. The value at array position j of the returned
   * CDF array is the sum of the returned values in positions 0 through j of the returned PMF
   * array. This can be viewed as array of ranks of the given split points plus one more value
   * that is always 1.
   */
  vector_double get_CDF(const T* split_points, uint32_t size, bool inclusive = true) const;

  /**
   * Returns an approximate quantile of the given normalized rank.
   * The normalized rank must be in the range [0.0, 1.0] (both inclusive).
   * <p>If the sketch is empty this throws std::runtime_error.
   *
   * @param rank of an item in the hypothetical sorted stream.
   * @param inclusive if true, the given rank is considered inclusive (includes weight of an item)
   *
   * @return approximate quantile associated with the given rank
   */
  quantile_return_type get_quantile(double rank, bool inclusive = true) const;

  /**
   * Returns an approximate lower bound of the given normalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param num_std_dev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate lower bound rank.
   */
  double get_rank_lower_bound(double rank, uint8_t num_std_dev) const;

  /**
   * Returns an approximate upper bound of the given normalized rank.
   * @param rank the given rank, a value between 0 and 1.0.
   * @param num_std_dev the number of standard deviations. Must be 1, 2, or 3.
   * @return an approximate upper bound rank.
   */
  double get_rank_upper_bound(double rank, uint8_t num_std_dev) const;

  /**
   * Returns an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   * Derived from Lemma 12 in https://arxiv.org/abs/2004.01668v2, but the constant factors were
   * modified based on empirical measurements.
   *
   * @param k the given value of k
   * @param rank the given normalized rank, a number in [0,1].
   * @param hra if true High Rank Accuracy mode is being selected, otherwise, Low Rank Accuracy.
   * @param n an estimate of the total number of items submitted to the sketch.
   * @return an a priori estimate of relative standard error (RSE, expressed as a number in [0,1]).
   */
  static double get_RSE(uint16_t k, double rank, bool hra, uint64_t n);

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for fixed-size arithmetic types (integral and floating point).
   * @param sd instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename TT = T, typename SerDe = serde<T>, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for all other types and can be expensive since every item needs to be looked at.
   * @param sd instance of a SerDe
   * @return size in bytes needed to serialize this sketch
   */
  template<typename TT = T, typename SerDe = serde<T>, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes(const SerDe& sd = SerDe()) const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   * @param sd instance of a SerDe
   */
  template<typename SerDe = serde<T>>
  void serialize(std::ostream& os, const SerDe& sd = SerDe()) const;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is a blank space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   * @param sd instance of a SerDe
   */
  template<typename SerDe = serde<T>>
  vector_bytes serialize(unsigned header_size_bytes = 0, const SerDe& sd = SerDe()) const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @param sd instance of a SerDe
   * @param comparator instance of a Comparator
   * @param allocator instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = serde<T>>
  static req_sketch deserialize(std::istream& is, const SerDe& sd = SerDe(),
      const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param sd instance of a SerDe
   * @param comparator instance of a Comparator
   * @param allocator instance of an Allocator
   * @return an instance of a sketch
   */
  template<typename SerDe = serde<T>>
  static req_sketch deserialize(const void* bytes, size_t size, const SerDe& sd = SerDe(),
      const Comparator& comparator = Comparator(), const Allocator& allocator = Allocator());

  /**
   * Prints a summary of the sketch.
   * @param print_levels if true include information about levels
   * @param print_items if true include sketch data
   */
  string<Allocator> to_string(bool print_levels = false, bool print_items = false) const;

  class const_iterator;

  /**
   * Iterator pointing to the first item in the sketch.
   * If the sketch is empty, the returned iterator must not be dereferenced or incremented.
   * @return iterator pointing to the first item in the sketch
   */
  const_iterator begin() const;

  /**
   * Iterator pointing to the past-the-end item in the sketch.
   * The past-the-end item is the hypothetical item that would follow the last item.
   * It does not point to any item, and must not be dereferenced or incremented.
   * @return iterator pointing to the past-the-end item in the sketch
   */
  const_iterator end() const;

  /**
   * Gets the sorted view of this sketch
   * @return the sorted view of this sketch
   */
  quantiles_sorted_view<T, Comparator, Allocator> get_sorted_view() const;

private:
  Comparator comparator_;
  Allocator allocator_;
  uint16_t k_;
  bool hra_;
  uint32_t max_nom_size_;
  uint32_t num_retained_;
  uint64_t n_;
  std::vector<Compactor, AllocCompactor> compactors_;
  optional<T> min_item_;
  optional<T> max_item_;
  mutable quantiles_sorted_view<T, Comparator, Allocator>* sorted_view_;

  void setup_sorted_view() const; // modifies mutable state
  void reset_sorted_view();

  static const bool LAZY_COMPRESSION = false;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t FAMILY = 17;
  static const size_t PREAMBLE_SIZE_BYTES = 8;
  enum flags { RESERVED1, RESERVED2, IS_EMPTY, IS_HIGH_RANK, RAW_ITEMS, IS_LEVEL_ZERO_SORTED };

  static constexpr double FIXED_RSE_FACTOR = 0.084;
  static double relative_rse_factor();

  uint8_t get_num_levels() const;
  void grow();
  void update_max_nom_size();
  void update_num_retained();
  void compress();

  static double get_rank_lb(uint16_t k, uint8_t num_levels, double rank, uint8_t num_std_dev, uint64_t n, bool hra);
  static double get_rank_ub(uint16_t k, uint8_t num_levels, double rank, uint8_t num_std_dev, uint64_t n, bool hra);
  static bool is_exact_rank(uint16_t k, uint8_t num_levels, double rank, uint64_t n, bool hra);

  // for deserialization
  req_sketch(uint16_t k, bool hra, uint64_t n,
      optional<T>&& min_item, optional<T>&& max_item,
      std::vector<Compactor, AllocCompactor>&& compactors, const Comparator& comparator);

  static void check_preamble_ints(uint8_t preamble_ints, uint8_t num_levels);
  static void check_serial_version(uint8_t serial_version);
  static void check_family_id(uint8_t family_id);

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_item(const TT& item) {
    return !std::isnan(item);
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_item(const TT&) {
    return true;
  }

  // for type converting constructor
  template<typename TT, typename CC, typename AA> friend class req_sketch;
};

template<typename T, typename C, typename A>
class req_sketch<T, C, A>::const_iterator {
public:
  using iterator_category = std::input_iterator_tag;
  using value_type = std::pair<const T&, const uint64_t>;
  using difference_type = void;
  using pointer = const return_value_holder<value_type>;
  using reference = const value_type;

  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  reference operator*() const;
  pointer operator->() const;
private:
  using LevelsIterator = typename std::vector<Compactor, AllocCompactor>::const_iterator;
  LevelsIterator levels_it_;
  LevelsIterator levels_end_;
  const T* compactor_it_;
  friend class req_sketch<T, C, A>;
  const_iterator(LevelsIterator begin, LevelsIterator end);
};

} /* namespace datasketches */

#include "req_sketch_impl.hpp"

#endif
