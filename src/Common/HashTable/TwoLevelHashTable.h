#pragma once

#include <type_traits>
#include <vector>
#include <base/defines.h>
#include <Common/CacheLine.h>
#include <Common/HashTable/HashTable.h>


/** Two-level hash table.
  * Represents 256 (or 1 << bits_for_bucket) small hash tables (buckets of the first level).
  * To determine which one to use, one of the bytes of the hash function is taken.
  *
  * Usually works a little slower than a simple hash table.
  * However, it has advantages in some cases:
  * - if you need to merge two hash tables together, then you can easily parallelize it by buckets;
  * - delay during resizes is amortized, since the small hash tables will be resized separately;
  * - in theory, resizes are cache-local in a larger range of sizes.
  *
  * With `bits_for_bucket = 0` there is a single bucket: routing folds to a constant.
  * Lookups and inserts compile down to what the single-level table does.
  * One map type can then serve both a serial fill and a fill from many threads.
  *
  * `BucketHash` selects the bucket when the hash a cell is placed by is a poor bucket selector.
  * A `FixedHashMap` places by the key itself.
  * Routing on the high bits of a dense key range would put every key into one bucket.
  * See `PartitionedFixedHashMap`.
  *
  * Buckets share no state.
  * Threads may fill different buckets at the same time when every bucket is written under its own lock.
  * Call `computeBucketPrefix` before reading `offsetInternal`.
  */

template <size_t initial_size_degree = 8>
struct TwoLevelHashTableGrower : public HashTableGrowerWithPrecalculation<initial_size_degree>
{
    /// Increase the size of the hash table.
    void increaseSize() { this->increaseSizeDegree(this->sizeDegree() >= 15 ? 1 : 2); }
};

constexpr Int32 DEFAULT_BITS_FOR_BUCKET = 8;

/// A table that directly addresses a fixed key range, so that all buckets can share one instance of
/// it. Specialized next to the table types that qualify.
template <typename Impl>
struct IsFixedRangeTable : std::false_type
{
};

/// Bucket selection for a fixed-range table, whose placement "hash" is the key itself. Hashes the
/// cache line the key's cell starts on: a dense key range spreads over the buckets, and two keys
/// whose cells share a line stay in one bucket, so they are never written under two different locks.
/// The line is the cell's byte offset divided by the line size, not the key divided by a cells-per-line
/// count, because `cell_size` does not always divide the line size.
template <size_t cell_size>
struct FixedRangeBucketHash
{
    template <typename Key>
    size_t ALWAYS_INLINE operator()(Key key) const
    {
        const UInt64 line = (static_cast<UInt64>(key) * cell_size) / DB::CH_CACHE_LINE_SIZE;
        return static_cast<size_t>((line * 0x9E3779B97F4A7C15ULL) >> 32);
    }
};

template <
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator,
    typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>,
    Int32 bits_for_bucket = DEFAULT_BITS_FOR_BUCKET,
    typename BucketHash = void>
class TwoLevelHashTable : private boost::noncopyable, protected Hash /// empty base optimization
{
    static_assert(bits_for_bucket >= 0 && bits_for_bucket < 32, "the bucket is taken from the low 32 bits of the hash");

protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = TwoLevelHashTable;

public:
    using Impl = ImplTable;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << bits_for_bucket;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    static constexpr bool isFixedRangeStorage() { return IsFixedRangeTable<ImplTable>::value; }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static constexpr UInt32 bucketShift() { return 32 - bits_for_bucket; }
    static size_t ALWAYS_INLINE getBucketFromHash(size_t hash_value) { return (hash_value >> bucketShift()) & MAX_BUCKET; }

private:
    /// One sub-table per bucket.
    class PerBucketStorage
    {
    public:
        PerBucketStorage() = default;
        explicit PerBucketStorage(size_t size_hint) { reserve(size_hint); }

        Impl & operator[](size_t bucket) { return buckets[bucket]; }
        const Impl & operator[](size_t bucket) const { return buckets[bucket]; }

        static constexpr UInt32 iterationBuckets() { return NUM_BUCKETS; }
        static constexpr UInt32 lastIterationBucket() { return MAX_BUCKET; }

        void reserve(size_t num_elements)
        {
            for (auto & bucket : buckets)
                bucket.reserve(num_elements / NUM_BUCKETS);
        }

        size_t size() const { return sumOverBuckets([](const Impl & bucket) { return bucket.size(); }); }
        size_t getBufferSizeInBytes() const { return sumOverBuckets([](const Impl & bucket) { return bucket.getBufferSizeInBytes(); }); }
        size_t getBufferSizeInCells() const { return sumOverBuckets([](const Impl & bucket) { return bucket.getBufferSizeInCells(); }); }

        bool empty() const
        {
            for (const auto & bucket : buckets)
                if (!bucket.empty())
                    return false;
            return true;
        }

        template <typename Func>
        void ALWAYS_INLINE forEachMapped(Func && func)
        {
            for (auto & bucket : buckets)
                bucket.forEachMapped(func);
        }

        /// The bounds are per bucket here, so nothing to restore.
        void restoreMinMaxOptimization() { }
        static bool canUseMinMaxOptimization() { return false; }

        /// Prefix sums of the bucket capacities: `bucket_cells_prefix[b]` is the number of cells in
        /// the buckets before `b`. Must not run while another thread reads offsets.
        void computeBucketPrefix() const
        {
            bucket_cells_prefix.assign(NUM_BUCKETS, 0);
            size_t run = 0;
            for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            {
                bucket_cells_prefix[i] = run;
                run += buckets[i].getBufferSizeInCells();
            }
        }

        size_t offsetInternal(typename Impl::ConstLookupResult ptr, size_t bucket) const
        {
            if (ptr->isZero(buckets[bucket]))
                return 0;
            if constexpr (NUM_BUCKETS == 1)
                return static_cast<size_t>(ptr - buckets[0].buf) + 1;

            chassert(!bucket_cells_prefix.empty(), "computeBucketPrefix must run before an offset is read");
            return bucket_cells_prefix[bucket] + static_cast<size_t>(ptr - buckets[bucket].buf) + 1;
        }

    private:
        template <typename Get>
        size_t sumOverBuckets(Get && get) const
        {
            size_t res = 0;
            for (const auto & bucket : buckets)
                res += get(bucket);
            return res;
        }

        Impl buckets[NUM_BUCKETS];
        mutable std::vector<size_t> bucket_cells_prefix;
    };

    /// One flat table that every bucket maps into. The buckets only partition the keys.
    /// A caller can fill from several threads under one lock per bucket.
    /// Distinct keys are distinct cells, and the table counts its size with an atomic.
    /// The min/max bounds of the flat table are the one thing those writers would race on.
    /// They stay off while there is more than one bucket.
    /// `restoreMinMaxOptimization` derives them again after the fill.
    class FixedRangeStorage
    {
    public:
        FixedRangeStorage()
        {
            if constexpr (NUM_BUCKETS > 1)
                flat.disableMinMaxOptimization();
        }

        explicit FixedRangeStorage(size_t /* size_hint */) : FixedRangeStorage() { }

        Impl & operator[](size_t) { return flat; }
        const Impl & operator[](size_t) const { return flat; }

        static constexpr UInt32 iterationBuckets() { return 1; }
        static constexpr UInt32 lastIterationBucket() { return 0; }

        void reserve(size_t) { }

        size_t size() const { return flat.size(); }
        bool empty() const { return flat.empty(); }
        size_t getBufferSizeInBytes() const { return flat.getBufferSizeInBytes(); }
        size_t getBufferSizeInCells() const { return flat.getBufferSizeInCells(); }

        template <typename Func>
        void ALWAYS_INLINE forEachMapped(Func && func)
        {
            flat.forEachMapped(func);
        }

        void restoreMinMaxOptimization() { flat.restoreMinMaxOptimization(); }
        bool canUseMinMaxOptimization() const { return flat.canUseMinMaxOptimization(); }

        void computeBucketPrefix() const { }
        size_t offsetInternal(typename Impl::ConstLookupResult ptr, size_t) const { return flat.offsetInternal(ptr); }

    private:
        Impl flat;
    };

    using Storage = std::conditional_t<isFixedRangeStorage(), FixedRangeStorage, PerBucketStorage>;

    /// A fixed-range cell stores no key; the cell index the sub-table iterator reports as the hash is the key.
    template <typename ImplIterator>
    static size_t ALWAYS_INLINE routedBucketFromIteration(const ImplIterator & current_it, size_t physical_bucket)
    {
        if constexpr (NUM_BUCKETS == 1)
            return 0;
        else if constexpr (isFixedRangeStorage())
            return getBucketFromHash(bucketRoutingHash(static_cast<Key>(current_it.getHash()), current_it.getHash()));
        else
            return physical_bucket;
    }

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Storage impls;

    TwoLevelHashTable() = default;

    explicit TwoLevelHashTable(size_t size_hint) : impls(size_hint) { }

    /// Copy the data from another (normal) hash table. It should have the same hash function.
    /// The constraint keeps an integer size hint from choosing this overload over the one above.
    template <typename Source>
    requires(!std::is_arithmetic_v<Source>)
    explicit TwoLevelHashTable(const Source & src)
    {
        typename Source::const_iterator it = src.begin();

        /// It is assumed that the zero key (stored separately) is first in iteration order.
        if (it != src.end() && it.getPtr()->isZero(src))
        {
            insert(it->getValue());
            ++it;
        }

        for (; it != src.end(); ++it)
        {
            const Cell * cell = it.getPtr();
            size_t hash_value = cell->getHash(src);
            size_t buck = bucketFor(cell->getKey(), hash_value);
            impls[buck].insertUniqueNonZero(cell, hash_value);
        }
    }

    /// Static so that a caller can route keys to buckets without a table at hand. `Hash` must be stateless.
    static size_t hash(const Key & x) { return Hash{}(x); }

    void reserve(size_t num_elements) { impls.reserve(num_elements); }

    /// The hash the bucket is selected by, given the hash the cell is placed by.
    template <typename K>
    static size_t ALWAYS_INLINE bucketRoutingHash(const K & key, size_t cell_hash_value)
    {
        if constexpr (std::is_void_v<BucketHash>)
            return cell_hash_value;
        else
            return BucketHash{}(key);
    }

    /// Index of the sub-table that holds `key`.
    /// Fixed-range storage is one table for every bucket, so it folds to zero like a single bucket.
    template <typename K>
    static size_t ALWAYS_INLINE bucketFor(const K & key, size_t hash_value)
    {
        if constexpr (isFixedRangeStorage() || NUM_BUCKETS == 1)
            return 0;
        else
            return getBucketFromHash(bucketRoutingHash(key, hash_value));
    }

    template <typename K>
    static size_t ALWAYS_INLINE bucketFor(const K & key)
    {
        if constexpr (isFixedRangeStorage() || NUM_BUCKETS == 1)
            return 0;
        else
            return getBucketFromHash(bucketRoutingHash(key, hash(key)));
    }

    size_t ALWAYS_INLINE bucketOf(ConstLookupResult ptr) const
    {
        if constexpr (isFixedRangeStorage() || NUM_BUCKETS == 1)
            return 0;
        else
            return getBucketFromHash(bucketRoutingHash(ptr->getKey(), ptr->getHash(*this)));
    }

protected:
    typename Impl::iterator beginOfNextNonEmptyBucket(size_t & bucket)
    {
        while (bucket != impls.iterationBuckets() && impls[bucket].empty())
            ++bucket;

        if (bucket != impls.iterationBuckets())
            return impls[bucket].begin();

        --bucket;
        return impls[impls.lastIterationBucket()].end();
    }

    typename Impl::const_iterator beginOfNextNonEmptyBucket(size_t & bucket) const
    {
        while (bucket != impls.iterationBuckets() && impls[bucket].empty())
            ++bucket;

        if (bucket != impls.iterationBuckets())
            return impls[bucket].begin();

        --bucket;
        return impls[impls.lastIterationBucket()].end();
    }

public:
    class iterator /// NOLINT
    {
        Self * container{};
        size_t bucket{};
        typename Impl::iterator current_it{};

        friend class TwoLevelHashTable;

        iterator(Self * container_, size_t bucket_, typename Impl::iterator current_it_)
            : container(container_), bucket(bucket_), current_it(current_it_) {}

    public:
        iterator() = default;

        bool operator== (const iterator & rhs) const { return bucket == rhs.bucket && current_it == rhs.current_it; }
        bool operator!= (const iterator & rhs) const { return !(*this == rhs); }

        iterator & operator++()
        {
            ++current_it;
            if (current_it == container->impls[bucket].end())
            {
                ++bucket;
                current_it = container->beginOfNextNonEmptyBucket(bucket);
            }

            return *this;
        }

        Cell & operator* () const { return *current_it; }
        Cell * operator->() const { return current_it.getPtr(); }

        Cell * getPtr() const { return current_it.getPtr(); }
        size_t getHash() const { return current_it.getHash(); }
        /// The sub-table being iterated. With fixed-range storage this is always 0.
        size_t getBucket() const { return bucket; }
        /// The bucket the key routes to, which is what partitions a scan of a fixed-range table.
        size_t getRoutedBucket() const { return Self::routedBucketFromIteration(current_it, bucket); }
    };


    class const_iterator /// NOLINT
    {
        const Self * container{};
        size_t bucket{};
        typename Impl::const_iterator current_it{};

        friend class TwoLevelHashTable;

        const_iterator(const Self * container_, size_t bucket_, typename Impl::const_iterator current_it_)
            : container(container_), bucket(bucket_), current_it(current_it_)
        {
        }

    public:
        const_iterator() = default;
        const_iterator(const iterator & rhs) : container(rhs.container), bucket(rhs.bucket), current_it(rhs.current_it) {} /// NOLINT

        bool operator== (const const_iterator & rhs) const { return bucket == rhs.bucket && current_it == rhs.current_it; }
        bool operator!= (const const_iterator & rhs) const { return !(*this == rhs); }

        const_iterator & operator++()
        {
            ++current_it;
            if (current_it == container->impls[bucket].end())
            {
                ++bucket;
                current_it = container->beginOfNextNonEmptyBucket(bucket);
            }

            return *this;
        }

        const Cell & operator* () const { return *current_it; }
        const Cell * operator->() const { return current_it.getPtr(); }

        const Cell * getPtr() const { return current_it.getPtr(); }
        size_t getHash() const { return current_it.getHash(); }
        size_t getBucket() const { return bucket; }
        size_t getRoutedBucket() const { return Self::routedBucketFromIteration(current_it, bucket); }
    };


    const_iterator begin() const
    {
        size_t buck = 0;
        typename Impl::const_iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return { this, buck, impl_it };
    }

    iterator begin()
    {
        size_t buck = 0;
        typename Impl::iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return { this, buck, impl_it };
    }

    const_iterator end() const { return { this, impls.lastIterationBucket(), impls[impls.lastIterationBucket()].end() }; }
    iterator end() { return { this, impls.lastIterationBucket(), impls[impls.lastIterationBucket()].end() }; }

    const_iterator iteratorAt(size_t bucket) const
    {
        if (bucket >= impls.iterationBuckets())
            return end();
        auto impl_it = beginOfNextNonEmptyBucket(bucket);
        return { this, bucket, impl_it };
    }

    iterator iteratorAt(size_t bucket)
    {
        if (bucket >= impls.iterationBuckets())
            return end();
        auto impl_it = beginOfNextNonEmptyBucket(bucket);
        return { this, bucket, impl_it };
    }


    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        const auto & key = Cell::getKey(x);
        size_t hash_value = hash(key);

        std::pair<LookupResult, bool> res;
        emplace(key, res.first, res.second, hash_value);

        if (res.second)
            res.first->setMapped(x);

        return res;
    }

    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const Cell & cell)
    {
        auto hash_value = cell.getHash(*this);

        std::pair<LookupResult, bool> res;
        emplace(cell.getKey(), res.first, res.second, hash_value);

        if (res.second)
            res.first->setMapped(cell.getValue());

        return res;
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder && key_holder) const
    requires requires(const Impl & impl, size_t key_hash) { impl.prefetchByHash(key_hash); }
    {
        const auto & key = keyHolderGetKey(key_holder);
        const auto key_hash = hash(key);
        impls[bucketFor(key, key_hash)].prefetchByHash(key_hash);
        /// Release any temporary key memory held by the holder (e.g. `SerializedKeyHolder` rolls back the Arena allocation).
        keyHolderDiscardKey(key_holder);
    }

    /// The two methods below answer from the cell hash alone.
    /// With a `BucketHash` the hash does not identify the bucket.
    /// With fixed-range storage there is no hashed placement to prefetch.
    /// They then do nothing, and `isEmptyCell` answers "not known to be empty".
    void ALWAYS_INLINE prefetchByHash(size_t key_hash) const
    {
        if constexpr (!isFixedRangeStorage() && std::is_void_v<BucketHash>)
            impls[getBucketFromHash(key_hash)].prefetchByHash(key_hash);
    }

    bool ALWAYS_INLINE isEmptyCell(size_t key_hash) const
    {
        if constexpr (!isFixedRangeStorage() && std::is_void_v<BucketHash>)
            return impls[getBucketFromHash(key_hash)].isEmptyCell(key_hash);
        else
            return false;
    }

    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` values if you inserted a new key,
      * since when destroying a hash table, the destructor will be invoked for it!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      *
      * Only the bucket of the key is touched, so callers that hold one lock per bucket may insert
      * into different buckets at the same time.
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        size_t hash_value = hash(keyHolderGetKey(key_holder));
        emplace(key_holder, it, inserted, hash_value);
    }


    /// Same, but with a precalculated values of hash function.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it,
                                  bool & inserted, size_t hash_value)
    {
        impls[bucketFor(keyHolderGetKey(key_holder), hash_value)].emplace(key_holder, it, inserted, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value)
    {
        return impls[bucketFor(x, hash_value)].find(x, hash_value);
    }

    ConstLookupResult ALWAYS_INLINE find(Key x, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x) { return find(x, hash(x)); }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return find(x, hash(x)); }

    bool ALWAYS_INLINE has(const Key & x) const { return impls[bucketFor(x)].has(x); }

    bool ALWAYS_INLINE erase(Key x, size_t hash_value)
    {
        return impls[bucketFor(x, hash_value)].erase(x, hash_value);
    }

    bool ALWAYS_INLINE erase(Key x) { return erase(x, hash(x)); }


    /// Fixed-range storage is one table however many buckets route into it, so it is serialized once.
    static constexpr UInt32 serializedPartitionCount() { return isFixedRangeStorage() ? 1 : NUM_BUCKETS; }

    void write(DB::WriteBuffer & wb) const
    {
        for (UInt32 i = 0; i < serializedPartitionCount(); ++i)
            impls[i].write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        for (UInt32 i = 0; i < serializedPartitionCount(); ++i)
        {
            if (i != 0)
                DB::writeChar(',', wb);
            impls[i].writeText(wb);
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        for (UInt32 i = 0; i < serializedPartitionCount(); ++i)
            impls[i].read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        for (UInt32 i = 0; i < serializedPartitionCount(); ++i)
        {
            if (i != 0)
                DB::assertChar(',', rb);
            impls[i].readText(rb);
        }
    }


    size_t size() const { return impls.size(); }
    bool empty() const { return impls.empty(); }
    size_t getBufferSizeInBytes() const { return impls.getBufferSizeInBytes(); }
    size_t getBufferSizeInCells() const { return impls.getBufferSizeInCells(); }

    /// Walk the mapped values of every bucket.
    /// Defined here so a two-level table over set buckets also satisfies generic mapped-value walks.
    /// Set buckets have no mapped values, so this visits nothing.
    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        impls.forEachMapped(func);
    }

    /// Prefix sums that `offsetInternal` uses to number cells across all buckets.
    /// Call this once the table stops growing, and again after it grows.
    /// An offset read before that is stale. The lookup path does not check.
    void computeBucketPrefix() const { impls.computeBucketPrefix(); }

    void restoreMinMaxOptimization() { impls.restoreMinMaxOptimization(); }
    bool canUseMinMaxOptimization() const { return impls.canUseMinMaxOptimization(); }

    /// Number of the cell over all buckets.
    /// 0 for the zero cell, otherwise the position in the concatenated bucket buffers plus one.
    /// That fits an array of `getBufferSizeInCells() + 1`.
    /// `computeBucketPrefix` must have run since the last insert; a single bucket needs none.
    size_t offsetInternal(ConstLookupResult ptr) const { return impls.offsetInternal(ptr, bucketOf(ptr)); }

    /// Cell number when the caller already knows the bucket of `ptr`.
    size_t ALWAYS_INLINE offsetInternalAtBucket(ConstLookupResult ptr, size_t iteration_bucket) const
    {
        return impls.offsetInternal(ptr, iteration_bucket);
    }
};
