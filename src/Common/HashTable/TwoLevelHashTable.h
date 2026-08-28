#pragma once

#include <base/defines.h>
#include <vector>
#include <type_traits>
#include <Common/HashTable/HashTable.h>

/** Two-level hash table.
  * Represents 256 (or 1ULL << BITS_FOR_BUCKET) small hash tables (buckets of the first level).
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

template <typename Impl>
struct IsDirectAddressedTable : std::false_type
{
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

    static constexpr bool isFixedRangeStorage() { return IsDirectAddressedTable<ImplTable>::value; }

    static constexpr UInt32 numBuckets() { return static_cast<UInt32>(1) << static_cast<UInt32>(bits_for_bucket); }
    static constexpr UInt32 maxBucket() { return numBuckets() - 1; }
    static constexpr UInt32 bucketShift() { return 32 - bits_for_bucket; }
    static size_t ALWAYS_INLINE getBucketFromHash(size_t hash_value) { return (hash_value >> bucketShift()) & maxBucket(); }

private:
    class BucketPrefixSums
    {
    public:
        template <typename BucketAt>
        void compute(UInt32 bucket_count, BucketAt && bucket_at)
        {
            prefix.assign(bucket_count, 0);
            size_t run = 0;
            for (UInt32 i = 0; i < bucket_count; ++i)
            {
                prefix[i] = run;
                run += bucket_at(i).getBufferSizeInCells();
            }
            computed = true;
        }

        size_t offsetUnsafe(size_t buck, size_t cell_offset) const
        {
            chassert(computed);
            return prefix[buck] + cell_offset;
        }

    private:
        std::vector<size_t> prefix;
        bool computed = false;
    };

    class FixedStorage
    {
    public:
        FixedStorage() = default;

        explicit FixedStorage(size_t size_hint) { reserve(size_hint); }

        Impl & operator[](size_t bucket) { return buckets[bucket]; }
        const Impl & operator[](size_t bucket) const { return buckets[bucket]; }

        void reserve(size_t num_elements)
        {
            for (auto & bucket : buckets)
                bucket.reserve(num_elements / numBuckets());
        }

        void computeBucketPrefix() const
        {
            prefix_sums.compute(numBuckets(), [this](UInt32 i) -> const Impl & { return buckets[i]; });
        }

        /// Requires `computeBucketPrefix` to have run; `HashJoin` freezes the maps before any
        /// probe, and `offsetUnsafe` chasserts it.
        size_t offsetInternal(typename Impl::ConstLookupResult ptr, size_t buck) const
        {
            return offsetInternalUnsafe(ptr, buck);
        }

        size_t offsetInternalUnsafe(typename Impl::ConstLookupResult ptr, size_t buck) const
        {
            if (ptr->isZero(buckets[buck]))
                return 0;
            if constexpr (numBuckets() == 1)
                return static_cast<size_t>(ptr - buckets[0].buf) + 1;
            return prefix_sums.offsetUnsafe(buck, static_cast<size_t>(ptr - buckets[buck].buf) + 1);
        }

        static constexpr UInt32 iterationBuckets() { return numBuckets(); }
        static constexpr UInt32 lastIterationBucket() { return maxBucket(); }

        template <typename Get>
        size_t ALWAYS_INLINE sumOverBuckets(Get && get) const
        {
            size_t res = 0;
            for (UInt32 i = 0; i < numBuckets(); ++i)
                res += get(buckets[i]);
            return res;
        }

        size_t size() const { return sumOverBuckets([](const Impl & b) { return b.size(); }); }

        bool empty() const
        {
            for (UInt32 i = 0; i < numBuckets(); ++i)
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
            for (UInt32 i = 0; i < numBuckets(); ++i)
                buckets[i].forEachMapped(func);
        }

    private:
        Impl buckets[numBuckets()];
        mutable BucketPrefixSums prefix_sums;
    };

    class FixedRangeStorage
    {
    public:
        /// One flat buffer that every bucket maps into; the buckets only partition the locks. The
        /// min/max bounds are the one thing inserts under different locks would race on.
        FixedRangeStorage()
        {
            if constexpr (numBuckets() > 1)
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
        size_t offsetInternalUnsafe(typename Impl::ConstLookupResult ptr, size_t) const { return flat.offsetInternal(ptr); }

    private:
        Impl flat;
    };

    using Storage = std::conditional_t<isFixedRangeStorage(), FixedRangeStorage, FixedStorage>;

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

    /// Static so that the join's build-side scatter can route without a table to ask. The instance
    /// is only there because some hashers, `TrivialHash` among them, are not static.
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

    /// Direct-addressed storage shares one buffer between its buckets and a single bucket needs no
    /// routing at all, so both fold to zero without the routing hash ever being computed.
    template <typename K>
    static size_t ALWAYS_INLINE bucketFor(const K & key, size_t hash_value)
    {
        if constexpr (isFixedRangeStorage() || numBuckets() == 1)
            return 0;
        else
            return getBucketFromHash(bucketRoutingHash(key, hash_value));
    }

    template <typename K>
    static size_t ALWAYS_INLINE bucketFor(const K & key)
    {
        if constexpr (isFixedRangeStorage() || numBuckets() == 1)
            return 0;
        else
            return getBucketFromHash(bucketRoutingHash(key, hash(key)));
    }

    size_t ALWAYS_INLINE bucketOf(ConstLookupResult ptr) const
    {
        if constexpr (isFixedRangeStorage() || numBuckets() == 1)
            return 0;
        else
            return getBucketFromHash(bucketRoutingHash(ptr->getKey(), ptr->getHash(*this)));
    }

private:
    /// Direct-addressed cells store no key (`VoidKey`); the impl iterator's `getHash()` is the cell index / key.
    template <typename ImplIt>
    static size_t ALWAYS_INLINE routedBucketFromIteration(const ImplIt & current_it, size_t physical_bucket)
    {
        if constexpr (numBuckets() == 1)
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
      * unsynchronized, provided the table already existed and `computeBucketPrefix` runs after.
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


    /// One part for direct-addressed storage: its buckets share a buffer.
    static constexpr UInt32 serializedPartitionCount() { return isFixedRangeStorage() ? 1 : numBuckets(); }

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

    size_t offsetInternal(ConstLookupResult ptr) const { return impls.offsetInternal(ptr, bucketOf(ptr)); }

    /// For an iterating caller that already knows the bucket. Needs current prefix sums.
    size_t ALWAYS_INLINE offsetInternalAtBucket(ConstLookupResult ptr, size_t iteration_bucket) const
    {
        return impls.offsetInternalUnsafe(ptr, iteration_bucket);
    }

    /// Skips the lazy `compute`: `computeBucketPrefix` must have run since the last capacity change.
    size_t offsetInternalUnsafe(ConstLookupResult ptr) const { return impls.offsetInternalUnsafe(ptr, bucketOf(ptr)); }
};
