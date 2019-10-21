#pragma once

#include <Common/HashTable/StringHashTable.h>

template <typename SubMaps, typename ImplTable = StringHashTable<SubMaps>, size_t BITS_FOR_BUCKET = 8>
class TwoLevelStringHashTable : private boost::noncopyable
{
protected:
    using HashValue = size_t;
    using Self = TwoLevelStringHashTable;

public:
    using Key = StringRef;
    using Impl = ImplTable;

    static constexpr size_t NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr size_t MAX_BUCKET = NUM_BUCKETS - 1;

    // TODO: currently hashing contains redundant computations when doing distributed or external aggregations
    size_t hash(const Key & x) const
    {
        return const_cast<Self &>(*this).dispatch(x, [&](const auto &, const auto &, size_t hash) { return hash; });
    }

    size_t operator()(const Key & x) const { return hash(x); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

public:
    using key_type = typename Impl::key_type;
    using value_type = typename Impl::value_type;

    Impl impls[NUM_BUCKETS];

    TwoLevelStringHashTable() {}

    template <typename Source>
    TwoLevelStringHashTable(const Source & src)
    {
        if (src.m0.hasZero())
            impls[0].m0.setHasZero(*src.m0.zeroValue());

        src.m1.forEachCell([&](auto & v)
        {
            size_t hash_value = v.getHash(src.m1);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m1.insertUniqueNonZero(&v, hash_value);
        });
        src.m2.forEachCell([&](auto & v)
        {
            size_t hash_value = v.getHash(src.m2);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m2.insertUniqueNonZero(&v, hash_value);
        });
        src.m3.forEachCell([&](auto & v)
        {
            size_t hash_value = v.getHash(src.m3);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m3.insertUniqueNonZero(&v, hash_value);
        });
        src.ms.forEachCell([&](auto & v)
        {
            size_t hash_value = v.getHash(src.ms);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].ms.insertUniqueNonZero(&v, hash_value);
        });
    }

    /// No fancy variants for TwoLevelHashTable as it's only used for aggregations.

    /// Iterate over every cell and pass non-zero cells to func.
    ///  Func should have signature void(const Cell &).
    template <typename Func>
    void forEachCell(Func && func) const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls[i].forEachCell(func);
    }

    /// Iterate over every cell and pass non-zero cells to func.
    ///  Func should have signature void(Cell &).
    template <typename Func>
    void forEachCell(Func && func)
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            impls[i].forEachCell(func);
    }

    // Dispatch is written in a way that maximizes the performance:
    // 1. Always memcpy 8 times bytes
    // 2. Use switch case extension to generate fast dispatching table
    // 3. Combine hash computation along with bucket computation and key loading
    // 4. Funcs are named callables that can be force_inlined
    // NOTE: It relies on Little Endianness and SSE4.2
    template <typename KeyHolder, typename Func>
    decltype(auto) ALWAYS_INLINE dispatch(KeyHolder && key_holder, Func && func)
    {
        auto & x = keyHolderGetKey(key_holder);
        size_t sz = x.size;
        if (!sz)
            return func(impls[0].m0, VoidKey{}, VoidHash{});
        const char * p = x.data;
        // pending bits that needs to be shifted out
        char s = (-sz & 7) * 8;
        size_t res, buck;
        union
        {
            StringKey8 k8;
            StringKey16 k16;
            StringKey24 k24;
            UInt64 n[3];
        };
        StringHashTableHash hash;
        const char * lp;

        switch (sz)
        {
            case 0: {
                // first half page
                if ((reinterpret_cast<uintptr_t>(p) & 2048) == 0)
                {
                    memcpy(&n[0], p, 8);
                    n[0] &= -1ul >> s;
                }
                else
                {
                    lp = x.data + x.size - 8;
                    memcpy(&n[0], lp, 8);
                    n[0] >>= s;
                }
                res = hash(k8);
                buck = getBucketFromHash(res);
                return func(impls[buck].m1, k8, res);
            }
            case 1: {
                memcpy(&n[0], p, 8);
                lp = x.data + x.size - 8;
                memcpy(&n[1], lp, 8);
                n[1] >>= s;
                res = hash(k16);
                buck = getBucketFromHash(res);
                return func(impls[buck].m2, k16, res);
            }
            case 2: {
                memcpy(&n[0], p, 16);
                lp = x.data + x.size - 8;
                memcpy(&n[2], lp, 8);
                n[2] >>= s;
                res = hash(k24);
                buck = getBucketFromHash(res);
                return func(impls[buck].m3, k24, res);
            }
            default: {
                res = hash(x);
                buck = getBucketFromHash(res);
                return func(impls[buck].ms, std::forward<KeyHolder>(key_holder), res);
            }
        }
    }

    using RawImpl = StringHashTable<SubMaps>;
    using LookupResult = typename RawImpl::LookupResult;
    using ConstLookupResult = typename RawImpl::ConstLookupResult;
    using EmplaceCallable = typename RawImpl::EmplaceCallable;
    using FindCallable = typename RawImpl::FindCallable;

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        dispatch(std::forward<KeyHolder>(key_holder), EmplaceCallable{it, inserted});
    }

    LookupResult ALWAYS_INLINE find(const Key & x) { return dispatch(x, FindCallable{}); }
    ConstLookupResult ALWAYS_INLINE find(const Key & x) const { return const_cast<std::decay_t<decltype(*this)> *>(this)->find(x); }

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
