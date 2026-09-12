#pragma once

#include <base/defines.h>
#include <vector>
#include <type_traits>
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
  * `bits_for_bucket = 0` gives a single bucket: routing folds to a constant and every operation
  * compiles down to the single-level table's, so a caller that wants no partitioning can stay on
  * this template rather than switch types.
  *
  * `BucketHash` overrides the hash the bucket is picked by, for when the placement hash is a poor
  * bucket selector - see `PartitionedFixedHashMap`.
  */

template <size_t initial_size_degree = 8>
struct TwoLevelHashTableGrower : public HashTableGrowerWithPrecalculation<initial_size_degree>
{
    /// Increase the size of the hash table.
    void increaseSize() { this->increaseSizeDegree(this->sizeDegree() >= 15 ? 1 : 2); }
};

constexpr int DEFAULT_BITS_FOR_BUCKET = 8;

/// A table that directly addresses a fixed range of keys, so all buckets can share one instance of it.
template <typename Impl>
struct IsFixedRangeTable : std::false_type
{
};

/// A fixed-range table places by the key itself, so routing on the high bits of that would put a dense
/// range in one bucket. Hashes the cache line the cell starts on, so two keys whose cells start on
/// the same line cannot sit under two locks. `key >> log2(line/sizeof(Cell))` only matches that
/// when `sizeof(Cell)` divides the line; `MapsOne` cells are 12 bytes (`bool` plus 8-byte `RowRef`).
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
class TwoLevelHashTable : private boost::noncopyable,
                          protected Hash /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = TwoLevelHashTable;
public:
    using Impl = ImplTable;

    static constexpr bool isFixedRangeStorage() { return IsFixedRangeTable<ImplTable>::value; }

    static constexpr UInt32 NUM_BUCKETS = 1ULL << bits_for_bucket;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;
    static constexpr UInt32 bucketShift() { return 32 - bits_for_bucket; }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t ALWAYS_INLINE getBucketFromHash(size_t hash_value) { return (hash_value >> bucketShift()) & MAX_BUCKET; }

private:
    class PerBucketStorage
    {
    public:
        PerBucketStorage() = default;

        explicit PerBucketStorage(size_t size_hint) { reserve(size_hint); }

        Impl & operator[](size_t bucket) { return buckets[bucket]; }
        const Impl & operator[](size_t bucket) const { return buckets[bucket]; }

        void reserve(size_t num_elements)
        {
            for (auto & bucket : buckets)
                bucket.reserve(num_elements / NUM_BUCKETS);
        }

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

        size_t offsetInternal(typename Impl::ConstLookupResult ptr, size_t buck) const
        {
            if (ptr->isZero(buckets[buck]))
                return 0;
            if constexpr (NUM_BUCKETS == 1)
                return static_cast<size_t>(ptr - buckets[0].buf) + 1;
            chassert(!bucket_cells_prefix.empty(), "computeBucketPrefix must run before an offset is read");
            return bucket_cells_prefix[buck] + static_cast<size_t>(ptr - buckets[buck].buf) + 1;
        }

        static constexpr UInt32 iterationBuckets() { return NUM_BUCKETS; }
        static constexpr UInt32 lastIterationBucket() { return MAX_BUCKET; }

        template <typename Get>
        size_t ALWAYS_INLINE sumOverBuckets(Get && get) const
        {
            size_t res = 0;
            for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
                res += get(buckets[i]);
            return res;
        }

        size_t size() const { return sumOverBuckets([](const Impl & b) { return b.size(); }); }

        bool empty() const
        {
            for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
                if (!buckets[i].empty())
                    return false;
            return true;
        }

        size_t getBufferSizeInBytes() const { return sumOverBuckets([](const Impl & b) { return b.getBufferSizeInBytes(); }); }
        size_t getBufferSizeInCells() const { return sumOverBuckets([](const Impl & b) { return b.getBufferSizeInCells(); }); }

        /// Only a shared buffer can carry min/max bounds across buckets.
        void restoreMinMaxOptimization() { }
        static bool canUseMinMaxOptimization() { return false; }

        template <typename Func>
        void ALWAYS_INLINE forEachMapped(Func && func)
        {
            for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
                buckets[i].forEachMapped(func);
        }

    private:
        Impl buckets[NUM_BUCKETS];
        /// Cells of every lower-numbered bucket, so `offsetInternal` can number cells table-wide.
        mutable std::vector<size_t> bucket_cells_prefix;
    };

    class FixedRangeStorage
    {
    public:
        /// One flat buffer that every bucket maps into; the buckets only partition the locks. The
        /// min/max bounds are the one thing inserts under different locks would race on.
        FixedRangeStorage()
        {
            if constexpr (NUM_BUCKETS > 1)
                flat.disableMinMaxOptimization();
        }

        explicit FixedRangeStorage(size_t /*size_hint*/)
            : FixedRangeStorage()
        {
        }

        Impl & operator[](size_t) { return flat; }
        const Impl & operator[](size_t) const { return flat; }

        static constexpr UInt32 iterationBuckets() { return 1; }
        static constexpr UInt32 lastIterationBucket() { return 0; }

        size_t size() const { return flat.size(); }
        bool empty() const { return flat.empty(); }
        size_t getBufferSizeInBytes() const { return flat.getBufferSizeInBytes(); }
        size_t getBufferSizeInCells() const { return flat.getBufferSizeInCells(); }

        template <typename Func>
        void ALWAYS_INLINE forEachMapped(Func && func)
        {
            flat.forEachMapped(func);
        }

        void reserve(size_t) { }
        void computeBucketPrefix() const { }
        void restoreMinMaxOptimization() { flat.restoreMinMaxOptimization(); }
        bool canUseMinMaxOptimization() const { return flat.canUseMinMaxOptimization(); }

        size_t offsetInternal(typename Impl::ConstLookupResult ptr, size_t) const { return flat.offsetInternal(ptr); }

    private:
        Impl flat;
    };

    using Storage = std::conditional_t<isFixedRangeStorage(), FixedRangeStorage, PerBucketStorage>;

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Storage impls;

    TwoLevelHashTable() = default;

    explicit TwoLevelHashTable(size_t size_hint)
        : impls(size_hint)
    {
    }

    /// Copy the data from another (normal) hash table. It should have the same hash function.
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

    /// Static so that the join's build-side scatter can route without a table to ask. `Hash{}`
    /// bypasses the empty-base `Hash` subobject, so `Hash` must be stateless.
    static size_t hash(const Key & x) { return Hash{}(x); }

    void reserve(size_t num_elements) { impls.reserve(num_elements); }

    template <typename K>
    static size_t ALWAYS_INLINE bucketRoutingHash(const K & key, size_t cell_hash_value)
    {
        if constexpr (std::is_void_v<BucketHash>)
            return cell_hash_value;
        else
            return BucketHash{}(key);
    }

    /// Fixed-range storage shares one buffer between its buckets and a single bucket needs no
    /// routing at all, so both fold to zero without the routing hash ever being computed.
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

private:
    /// Fixed-range cells store no key (`VoidKey`); the impl iterator's `getHash()` is the cell index / key.
    template <typename ImplIt>
    static size_t ALWAYS_INLINE routedBucketFromIteration(const ImplIt & current_it, size_t physical_bucket)
    {
        if constexpr (NUM_BUCKETS == 1)
            return 0;
        else if constexpr (isFixedRangeStorage())
            return getBucketFromHash(bucketRoutingHash(static_cast<key_type>(current_it.getHash()), current_it.getHash()));
        else
            return physical_bucket;
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
        size_t getBucket() const { return bucket; }
        /// Routing bucket for unmatched-row splits. `getBucket()` is the physical impls index.
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
        auto impl_it = beginOfNextNonEmptyBucket(buck);
        return { this, buck, impl_it };
    }

    iterator begin()
    {
        size_t buck = 0;
        auto impl_it = beginOfNextNonEmptyBucket(buck);
        return { this, buck, impl_it };
    }

    const_iterator end() const { return {this, impls.lastIterationBucket(), impls[impls.lastIterationBucket()].end()}; }
    iterator end() { return {this, impls.lastIterationBucket(), impls[impls.lastIterationBucket()].end()}; }

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
        const size_t hash_value = hash(key);
        std::pair<LookupResult, bool> res;
        emplace(key, res.first, res.second, hash_value);
        if (res.second)
            res.first->setMapped(x);

        return res;
    }

    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const Cell & cell)
    {
        const auto hash_value = cell.getHash(*this);
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
        keyHolderDiscardKey(key_holder);
    }

    void ALWAYS_INLINE prefetchByHash(size_t key_hash) const
    {
        if constexpr (isFixedRangeStorage())
            return;
        else if constexpr (!std::is_void_v<BucketHash>)
            return;
        else
            impls[getBucketFromHash(key_hash)].prefetchByHash(key_hash);
    }

    bool ALWAYS_INLINE isEmptyCell(size_t key_hash) const
    {
        if constexpr (isFixedRangeStorage())
            return false;
        else if constexpr (!std::is_void_v<BucketHash>)
            return false;
        else
            return impls[getBucketFromHash(key_hash)].isEmptyCell(key_hash);
    }

    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` values if you inserted a new key,
      * since when destroying a hash table, the destructor will be invoked for it!
      *
      * Only the target bucket is touched, so concurrent callers may insert into distinct buckets
      * unsynchronized; offsets are valid again only once `computeBucketPrefix` has run after the
      * last insert.
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        emplace(key_holder, it, inserted, hash(keyHolderGetKey(key_holder)));
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

    bool ALWAYS_INLINE erase(Key x, size_t hash_value)
    {
        return impls[bucketFor(x, hash_value)].erase(x, hash_value);
    }

    bool ALWAYS_INLINE erase(Key x) { return erase(x, hash(x)); }


    /// One part for fixed-range storage: its buckets share a buffer.
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

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func && func)
    {
        impls.forEachMapped(func);
    }

    bool ALWAYS_INLINE has(const Key & x) const
    {
        return impls[bucketFor(x)].has(x);
    }

    void computeBucketPrefix() const { impls.computeBucketPrefix(); }

    void restoreMinMaxOptimization() { impls.restoreMinMaxOptimization(); }
    bool canUseMinMaxOptimization() const { return impls.canUseMinMaxOptimization(); }

    /// `computeBucketPrefix` must have run since the last capacity change; `HashJoin` freezes the
    /// maps before any probe.
    size_t offsetInternal(ConstLookupResult ptr) const { return impls.offsetInternal(ptr, bucketOf(ptr)); }

    /// Same, for an iterating caller that already knows the bucket.
    size_t ALWAYS_INLINE offsetInternalAtBucket(ConstLookupResult ptr, size_t iteration_bucket) const
    {
        return impls.offsetInternal(ptr, iteration_bucket);
    }
};
