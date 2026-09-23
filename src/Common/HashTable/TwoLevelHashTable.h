#pragma once

#include <type_traits>
#include <vector>
#include <base/defines.h>
#include <Common/HashTable/HashTable.h>


/** Two-level hash table.
  * Represents 256 (or 1 << BITS_FOR_BUCKET) small hash tables (buckets of the first level).
  * To determine which one to use, one of the bytes of the hash function is taken.
  *
  * Usually works a little slower than a simple hash table.
  * However, it has advantages in some cases:
  * - if you need to merge two hash tables together, then you can easily parallelize it by buckets;
  * - delay during resizes is amortized, since the small hash tables will be resized separately;
  * - in theory, resizes are cache-local in a larger range of sizes.
  *
  * With `BITS_FOR_BUCKET = 0` there is a single bucket: routing folds to a constant.
  * Lookups and inserts compile down to what the single-level table does.
  * One map type can then serve both a serial fill and a fill from many threads.
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

constexpr size_t DEFAULT_BITS_FOR_BUCKET = 8;

template <
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator,
    typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>,
    size_t BITS_FOR_BUCKET = DEFAULT_BITS_FOR_BUCKET>
class TwoLevelHashTable : private boost::noncopyable, protected Hash /// empty base optimization
{
    static_assert(BITS_FOR_BUCKET < 32, "the bucket is taken from the low 32 bits of the hash");

protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = TwoLevelHashTable;

public:
    using Impl = ImplTable;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static constexpr UInt32 bucketShift() { return 32 - BITS_FOR_BUCKET; }
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

        /// Prefix sums of the bucket capacities: `bucket_cells_prefix[b]` is the number of cells in
        /// the buckets before `b`. Must not run while another thread reads offsets.
        void computeBucketPrefix()
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
        std::vector<size_t> bucket_cells_prefix;
    };

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    PerBucketStorage impls;

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
            impls[bucketFor(hash_value)].insertUniqueNonZero(cell, hash_value);
        }
    }

    /// Static so that a caller can route keys to buckets without a table at hand. `Hash` must be stateless.
    static size_t hash(const Key & x) { return Hash{}(x); }

    void reserve(size_t num_elements) { impls.reserve(num_elements); }

    /// Index of the sub-table that holds `key`. A single bucket needs no routing, so it folds to zero.
    static size_t ALWAYS_INLINE bucketFor(size_t hash_value)
    {
        if constexpr (NUM_BUCKETS == 1)
            return 0;
        else
            return getBucketFromHash(hash_value);
    }

    size_t ALWAYS_INLINE bucketOf(ConstLookupResult ptr) const { return bucketFor(ptr->getHash(*this)); }

protected:
    typename Impl::iterator beginOfNextNonEmptyBucket(size_t & bucket)
    {
        while (bucket != NUM_BUCKETS && impls[bucket].empty())
            ++bucket;

        if (bucket != NUM_BUCKETS)
            return impls[bucket].begin();

        --bucket;
        return impls[MAX_BUCKET].end();
    }

    typename Impl::const_iterator beginOfNextNonEmptyBucket(size_t & bucket) const
    {
        while (bucket != NUM_BUCKETS && impls[bucket].empty())
            ++bucket;

        if (bucket != NUM_BUCKETS)
            return impls[bucket].begin();

        --bucket;
        return impls[MAX_BUCKET].end();
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

    const_iterator end() const { return { this, MAX_BUCKET, impls[MAX_BUCKET].end() }; }
    iterator end() { return { this, MAX_BUCKET, impls[MAX_BUCKET].end() }; }

    const_iterator iteratorAt(size_t bucket) const
    {
        if (bucket >= NUM_BUCKETS)
            return end();
        auto impl_it = beginOfNextNonEmptyBucket(bucket);
        return { this, bucket, impl_it };
    }

    iterator iteratorAt(size_t bucket)
    {
        if (bucket >= NUM_BUCKETS)
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
        impls[bucketFor(key_hash)].prefetchByHash(key_hash);
        /// Release any temporary key memory held by the holder (e.g. `SerializedKeyHolder` rolls back the Arena allocation).
        keyHolderDiscardKey(key_holder);
    }

    void ALWAYS_INLINE prefetchByHash(size_t key_hash) const { impls[bucketFor(key_hash)].prefetchByHash(key_hash); }

    bool ALWAYS_INLINE isEmptyCell(size_t key_hash) const { return impls[bucketFor(key_hash)].isEmptyCell(key_hash); }

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
        impls[bucketFor(hash_value)].emplace(key_holder, it, inserted, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value)
    {
        return impls[bucketFor(hash_value)].find(x, hash_value);
    }

    ConstLookupResult ALWAYS_INLINE find(Key x, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x) { return find(x, hash(x)); }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return find(x, hash(x)); }

    bool ALWAYS_INLINE has(const Key & x) const { return impls[bucketFor(hash(x))].has(x); }

    bool ALWAYS_INLINE erase(Key x, size_t hash_value)
    {
        return impls[bucketFor(hash_value)].erase(x, hash_value);
    }

    bool ALWAYS_INLINE erase(Key x) { return erase(x, hash(x)); }


    void write(DB::WriteBuffer & wb) const
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            impls[i].write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::writeChar(',', wb);
            impls[i].writeText(wb);
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            impls[i].read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
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
    void computeBucketPrefix() { impls.computeBucketPrefix(); }

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
