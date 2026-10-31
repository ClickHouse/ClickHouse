#pragma once

#include <bit>
#include <type_traits>
#include <base/sanitizer_defs.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/FixedHashMap.h>
#include <Common/HashTable/FixedHashSet.h>


/** A `FixedHashTable` whose keys are split into buckets, for a caller that fills it from several
  * threads under one lock per bucket. It satisfies `BucketPartitionedTable`, the interface such a caller
  * relies on. It is not a drop-in `TwoLevelHashTable`: there is a single flat table, and the buckets
  * only decide which lock a key is inserted under. It has no `iteratorAt`, and its iteration order does
  * not follow the buckets, so a caller that scans by bucket must handle it separately.
  * Cells, offsets and iteration are those of the flat table. With `BITS_FOR_BUCKET = 0` it is the flat table.
  *
  * The flat table places a key at the cell with that index. Routing on the high bits of a dense key
  * range would put every key into one bucket, so a key is routed by the cache line its cell starts on.
  * With more than one bucket, the size of a cell divides the line or is a multiple of it.
  * So two keys on one line always share a bucket. `PartitionedFixedHashMapCell` pads a map cell to such a size.
  * `bucketOfKey` counts lines from the start of the buffer, so the routing assumes that the buffer starts on a
  * cache line. `FixedHashTable::alloc` does not request that alignment. If the buffer starts elsewhere, the data
  * stays correct, but writers under different bucket locks can share a line.
  *
  * A key always goes to one bucket, so writers under different bucket locks never write the same cell.
  * The table keeps no element counter: every thread would write it, and that one cache line would slow
  * the fill. `size` walks the cells instead, so do not call `size` or `empty` during the fill.
  * The min/max bounds of the flat table are the one thing concurrent writers would race on.
  * They stay off while there is more than one bucket. `restoreMinMaxOptimization` derives them after the fill.
  */
template <typename Impl, size_t BITS_FOR_BUCKET>
class PartitionedFixedHashTable
{
    static_assert(BITS_FOR_BUCKET < 32, "`NUM_BUCKETS` is a `UInt32`");

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;

    static_assert(
        NUM_BUCKETS == 1 || DB::CH_CACHE_LINE_SIZE % sizeof(cell_type) == 0 || sizeof(cell_type) % DB::CH_CACHE_LINE_SIZE == 0,
        "two keys on one cache line must share a bucket, so the size of a cell must divide the line or be a multiple of it");
    /// A cell that declares `padded_size` must have exactly that size, so a field added next to the padding fails here.
    static_assert(!requires { cell_type::padded_size; } || requires { requires sizeof(cell_type) == cell_type::padded_size; });

    /// The cell of a key is the cell with that index, so the key is the hash the flat table takes.
    static size_t hash(key_type key) { return key; }

    /// 2^64 divided by the golden ratio.
    static constexpr UInt64 FIBONACCI_MULTIPLIER = 0x9E3779B97F4A7C15ULL;

    /// `line` is the number of the cache line that the cell of `key` starts on.
    /// The bucket of `key` is the top `BITS_FOR_BUCKET` bits of `line * FIBONACCI_MULTIPLIER`.
    /// The multiply mixes every bit of `line` into the top bits.
    /// Taking the low bits of `line` instead would put keys at a power-of-two stride into one bucket.
    /// For example, with 16-byte cells and 16 buckets, the multiples of 256 are 64 lines apart,
    /// so all of them land in bucket 0.
    /// XOR with a constant cannot move a small line number into the top bits.
    /// The multiply is Fibonacci hashing and wraps by design, which is how it mixes the low bits of
    /// `line` into the top ones.
    static size_t ALWAYS_INLINE_NO_SANITIZE_UNSIGNED_OVERFLOW bucketOfKey(key_type key)
    {
        if constexpr (NUM_BUCKETS == 1)
            return 0;
        else
        {
            const UInt64 line = static_cast<UInt64>(key) * sizeof(cell_type) / DB::CH_CACHE_LINE_SIZE;
            return static_cast<size_t>((line * FIBONACCI_MULTIPLIER) >> (64 - BITS_FOR_BUCKET));
        }
    }

    template <typename ImplIterator>
    class BucketIterator : public ImplIterator /// NOLINT
    {
    public:
        BucketIterator() = default;
        explicit BucketIterator(const ImplIterator & it)
            : ImplIterator(it)
        {
        }

        BucketIterator & operator++()
        {
            ImplIterator::operator++();
            return *this;
        }

        /// The bucket the key routes to. There are no sub-tables, so a scan split by bucket filters on it.
        size_t getBucket() const { return bucketOfKey(static_cast<key_type>(this->getHash())); }
    };

    using iterator = BucketIterator<typename Impl::iterator>;
    using const_iterator = BucketIterator<typename Impl::const_iterator>;

    PartitionedFixedHashTable()
    {
        if constexpr (NUM_BUCKETS > 1)
            flat.disableMinMaxOptimization();
    }

    /// Neither copyable nor movable, like `TwoLevelHashTable`. A moved `FixedHashTable` would drop the disabled bounds.
    PartitionedFixedHashTable(const PartitionedFixedHashTable &) = delete;
    PartitionedFixedHashTable & operator=(const PartitionedFixedHashTable &) = delete;

    iterator begin() { return iterator(flat.begin()); }
    const_iterator begin() const { return const_iterator(flat.begin()); }
    iterator end() { return iterator(flat.end()); }
    const_iterator end() const { return const_iterator(flat.end()); }

    void ALWAYS_INLINE emplace(const key_type & key, LookupResult & it, bool & inserted, size_t hash_value = 0)
    {
        flat.emplace(key, it, inserted, hash_value);
    }

    LookupResult ALWAYS_INLINE find(const key_type & key) { return flat.find(key); }
    ConstLookupResult ALWAYS_INLINE find(const key_type & key) const { return flat.find(key); }
    LookupResult ALWAYS_INLINE find(const key_type & key, size_t hash_value) { return flat.find(key, hash_value); }
    ConstLookupResult ALWAYS_INLINE find(const key_type & key, size_t hash_value) const { return flat.find(key, hash_value); }
    bool ALWAYS_INLINE has(const key_type & key) const { return flat.has(key); }

    size_t size() const { return flat.size(); }
    bool empty() const { return flat.empty(); }
    size_t getBufferSizeInBytes() const { return flat.getBufferSizeInBytes(); }
    size_t getBufferSizeInCells() const { return flat.getBufferSizeInCells(); }

    /// A set has no mapped values, so this visits nothing, like `HashSetTable::forEachMapped`.
    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        if constexpr (!std::is_same_v<mapped_type, VoidMapped>)
            flat.forEachMapped(func);
    }

    /// The flat table numbers its cells itself, so there are no prefix sums to compute.
    void computeBucketPrefix() { }
    size_t offsetInternal(ConstLookupResult ptr) const { return flat.offsetInternal(ptr); }

    /// Call once no writer is left.
    void restoreMinMaxOptimization()
    {
        if constexpr (NUM_BUCKETS > 1)
            flat.restoreMinMaxOptimization();
    }

private:
    Impl flat;
};

template <typename T>
constexpr bool is_partitioned_fixed_table = false;

template <typename Impl, size_t BITS_FOR_BUCKET>
constexpr bool is_partitioned_fixed_table<PartitionedFixedHashTable<Impl, BITS_FOR_BUCKET>> = true;


template <size_t N>
struct PartitionedFixedHashMapCellPadding
{
    char bytes[N];
};

template <>
struct PartitionedFixedHashMapCellPadding<0>
{
};

/// `FixedHashMapCell` padded so that its size divides the cache line, or is a multiple of it when larger.
template <typename Key, typename Mapped>
struct PartitionedFixedHashMapCell : FixedHashMapCell<Key, Mapped>
{
    using Base = FixedHashMapCell<Key, Mapped>;
    using Base::Base;

    static constexpr size_t padded_size = sizeof(Base) <= DB::CH_CACHE_LINE_SIZE
        ? std::bit_ceil(sizeof(Base))
        : (sizeof(Base) + DB::CH_CACHE_LINE_SIZE - 1) / DB::CH_CACHE_LINE_SIZE * DB::CH_CACHE_LINE_SIZE;

    [[no_unique_address]] PartitionedFixedHashMapCellPadding<padded_size - sizeof(Base)> padding;
};

/// A `FixedHashMap` whose keys are split into buckets, so that several threads can fill it at once.
/// With one bucket there is nothing to route, so the cell is not padded.
template <typename Key, typename Mapped, size_t size_bits, size_t BITS_FOR_BUCKET>
using PartitionedFixedHashMap = PartitionedFixedHashTable<
    FixedHashMapWithSizeBitsAndCalculatedSize<
        Key,
        Mapped,
        size_bits,
        std::conditional_t<BITS_FOR_BUCKET == 0, FixedHashMapCell<Key, Mapped>, PartitionedFixedHashMapCell<Key, Mapped>>>,
    BITS_FOR_BUCKET>;

/// Set counterpart of `PartitionedFixedHashMap`. A set cell holds only the presence flag, so it needs no padding.
template <typename Key, size_t size_bits, size_t BITS_FOR_BUCKET>
using PartitionedFixedHashSet = PartitionedFixedHashTable<FixedHashSetWithSizeBitsAndCalculatedSize<Key, size_bits>, BITS_FOR_BUCKET>;
