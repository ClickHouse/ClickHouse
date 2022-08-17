#pragma once

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
  */

template <size_t initial_size_degree = 8>
struct TwoLevelHashTableGrower : public HashTableGrower<initial_size_degree>
{
    /// Increase the size of the hash table.
    void increaseSize()
    {
        this->size_degree += this->size_degree >= 15 ? 1 : 2;
    }
};

template
<
    typename Key,
    typename Cell,
    typename Hash,
    typename Grower,
    typename Allocator,
    typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>,
    size_t BITS_FOR_BUCKET = 8
>
class TwoLevelHashTable :
    private boost::noncopyable,
    protected Hash            /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = TwoLevelHashTable;
public:
    using Impl = ImplTable;

    static constexpr size_t NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr size_t MAX_BUCKET = NUM_BUCKETS - 1;

    size_t hash(const Key & x) const { return Hash::operator()(x); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

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
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Impl impls[NUM_BUCKETS];


    TwoLevelHashTable() {}

    /// Copy the data from another (normal) hash table. It should have the same hash function.
    template <typename Source>
    TwoLevelHashTable(const Source & src)
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
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].insertUniqueNonZero(cell, hash_value);
        }
    }


    class iterator
    {
        Self * container{};
        size_t bucket{};
        typename Impl::iterator current_it{};

        friend class TwoLevelHashTable;

        iterator(Self * container_, size_t bucket_, typename Impl::iterator current_it_)
            : container(container_), bucket(bucket_), current_it(current_it_) {}

    public:
        iterator() {}

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
    };


    class const_iterator
    {
        Self * container{};
        size_t bucket{};
        typename Impl::const_iterator current_it{};

        friend class TwoLevelHashTable;

        const_iterator(Self * container_, size_t bucket_, typename Impl::const_iterator current_it_)
            : container(container_), bucket(bucket_), current_it(current_it_) {}

    public:
        const_iterator() {}
        const_iterator(const iterator & rhs) : container(rhs.container), bucket(rhs.bucket), current_it(rhs.current_it) {}

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
        const Cell * operator->() const { return current_it->getPtr(); }

        const Cell * getPtr() const { return current_it.getPtr(); }
        size_t getHash() const { return current_it.getHash(); }
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

    const_iterator end() const         { return { this, MAX_BUCKET, impls[MAX_BUCKET].end() }; }
    iterator end()                     { return { this, MAX_BUCKET, impls[MAX_BUCKET].end() }; }


    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type & x)
    {
        size_t hash_value = hash(Cell::getKey(x));

        std::pair<LookupResult, bool> res;
        emplace(Cell::getKey(x), res.first, res.second, hash_value);

        if (res.second)
            insertSetMapped(res.first->getMapped(), x);

        return res;
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
        size_t buck = getBucketFromHash(hash_value);
        impls[buck].emplace(key_holder, it, inserted, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value)
    {
        size_t buck = getBucketFromHash(hash_value);
        return impls[buck].find(x, hash_value);
    }

    ConstLookupResult ALWAYS_INLINE find(Key x, size_t hash_value) const
    {
        return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x) { return find(x, hash(x)); }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return find(x, hash(x)); }


    void write(DB::WriteBuffer & wb) const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls[i].write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::writeChar(',', wb);
            impls[i].writeText(wb);
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls[i].read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::assertChar(',', rb);
            impls[i].readText(rb);
        }
    }


    size_t size() const
    {
        size_t res = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].size();

        return res;
    }

    bool empty() const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            if (!impls[i].empty())
                return false;

        return true;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t res = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].getBufferSizeInBytes();

        return res;
    }
};
