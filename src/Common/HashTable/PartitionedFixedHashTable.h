#pragma once

#include <type_traits>
#include <Common/CacheLine.h>
#include <Common/HashTable/FixedHashTable.h>


/** A `FixedHashTable` whose keys are split into buckets, for a caller that fills it from several
  * threads under one lock per bucket. It satisfies `BucketPartitionedTable`, the interface such a caller
  * relies on. It is not a drop-in `TwoLevelHashTable`: there is a single flat table, and the buckets
  * only decide which lock a key is inserted under. It has no `iteratorAt`, and its iteration order does
  * not follow the buckets, so a caller that scans by bucket must handle it separately.
  * Cells, offsets and iteration are those of the flat table. With `BITS_FOR_BUCKET = 0` it is the flat table.
  *
  * The flat table places a key at the cell with that index. Routing on the high bits of a dense key
  * range would put every key into one bucket, so a key is routed by the cache line its cell starts on.
  * `FixedHashMapCell` is padded so that its size divides the line, so two keys on one line share a bucket.
  *
  * Distinct keys are distinct cells, and `FixedHashTableStoredSize` counts the size with an atomic.
  * The min/max bounds of the flat table are the one thing concurrent writers would race on.
  * They stay off while there is more than one bucket. `restoreMinMaxOptimization` derives them after the fill.
  */
template <typename Impl, size_t BITS_FOR_BUCKET>
class PartitionedFixedHashTable
{
    static_assert(BITS_FOR_BUCKET < 32, "the bucket is taken from the low 32 bits of the hash");
    static_assert(
        std::is_base_of_v<FixedHashTableStoredSize<typename Impl::cell_type>, Impl>,
        "several threads insert at once, so the size counter has to be atomic");

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    /// The cell of a key is the cell with that index, so the key is the hash the flat table takes.
    static size_t hash(key_type key) { return key; }

    /// Hashes the number of the cache line the cell of `key` starts on.
    template <typename K>
    static size_t ALWAYS_INLINE bucketRoutingHash(const K & key, size_t /* hash_value */)
    {
        const UInt64 line = (static_cast<UInt64>(key) * sizeof(cell_type)) / DB::CH_CACHE_LINE_SIZE;
        /// `getBucketFromHash` keeps the high bits. A line number is small, so XOR cannot move it there.
        return static_cast<size_t>((line * 0x9E3779B97F4A7C15ULL) >> 32);
    }

    static size_t ALWAYS_INLINE getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

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
        size_t getBucket() const { return getBucketFromHash(bucketRoutingHash(this->getHash(), 0)); }
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
    void computeBucketPrefix() const { }
    size_t offsetInternal(ConstLookupResult ptr) const { return flat.offsetInternal(ptr); }
    size_t offsetInternalAtBucket(ConstLookupResult ptr, size_t /* bucket */) const { return flat.offsetInternal(ptr); }

    /// Call once no writer is left.
    void restoreMinMaxOptimization() { flat.restoreMinMaxOptimization(); }
    bool canUseMinMaxOptimization() const { return flat.canUseMinMaxOptimization(); }

private:
    Impl flat;
};
