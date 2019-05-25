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
        if (src.m0.size())
        {
            impls[0].m0.value = src.m0.value;
            impls[0].m0.is_empty = false;
        }
        for (auto & v : src.m1)
        {
            size_t hash_value = v.getHash(src.m1);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m1.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.m2)
        {
            size_t hash_value = v.getHash(src.m2);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m2.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.m3)
        {
            size_t hash_value = v.getHash(src.m3);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m3.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.ms)
        {
            size_t hash_value = v.getHash(src.ms);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].ms.insertUniqueNonZero(&v, hash_value);
        }
    }

    // Dispatch is written in a way that maximizes the performance:
    // 1. Always memcpy 8 times bytes
    // 2. Use switch case extension to generate fast dispatching table
    // 3. Combine hash computation along with bucket computation and key loading
    // 4. Funcs are named callables that can be force_inlined
    // NOTE: It relies on Little Endianness and SSE4.2
    template <typename Func>
    decltype(auto) ALWAYS_INLINE dispatch(Key x, Func && func)
    {
        static constexpr StringKey0 key0{};
        size_t sz = x.size;
        const char * p = x.data;
        // pending bits that needs to be shifted out
        char s = (-sz & 7) * 8;
        size_t res = -1ULL;
        size_t buck;
        UInt64 n[3];
        switch (sz)
        {
            case 0:
                return func(impls[0].m0, key0, 0);
            CASE_1_8 : {
                // first half page
                if ((reinterpret_cast<uintptr_t>(p) & 2048) == 0)
                {
                    memcpy(&n[0], p, 8);
                    n[0] &= -1ul >> s;
                }
                else
                {
                    const char * lp = x.data + x.size - 8;
                    memcpy(&n[0], lp, 8);
                    n[0] >>= s;
                }
                res = _mm_crc32_u64(res, n[0]);
                buck = getBucketFromHash(res);
                return func(impls[buck].m1, n[0], res);
            }
            CASE_9_16 : {
                memcpy(&n[0], p, 8);
                res = _mm_crc32_u64(res, n[0]);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[1], lp, 8);
                n[1] >>= s;
                res = _mm_crc32_u64(res, n[1]);
                buck = getBucketFromHash(res);
                return func(impls[buck].m2, *reinterpret_cast<StringKey16 *>(n), res);
            }
            CASE_17_24 : {
                memcpy(&n, p, 16);
                res = _mm_crc32_u64(res, n[0]);
                res = _mm_crc32_u64(res, n[1]);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[2], lp, 8);
                n[2] >>= s;
                res = _mm_crc32_u64(res, n[2]);
                buck = getBucketFromHash(res);
                return func(impls[buck].m3, *reinterpret_cast<StringKey24 *>(n), res);
            }
            default: {
                memcpy(&n, x.data, 24);
                res = _mm_crc32_u64(res, n[0]);
                res = _mm_crc32_u64(res, n[1]);
                res = _mm_crc32_u64(res, n[2]);
                p += 24;
                const char * lp = x.data + x.size - 8;
                while (p + 8 < lp)
                {
                    memcpy(&n[0], p, 8);
                    res = _mm_crc32_u64(res, n[0]);
                    p += 8;
                }
                memcpy(&n[0], lp, 8);
                n[0] >>= s;
                res = _mm_crc32_u64(res, n[0]);
                buck = getBucketFromHash(res);
                return func(impls[buck].ms, x, res);
            }
        }
    }

    using RawImpl = StringHashTable<SubMaps>;
    using EmplaceCallable = typename RawImpl::EmplaceCallable;
    using EmplaceCallableWithPool = typename RawImpl::EmplaceCallableWithPool;
    using FindCallable = typename RawImpl::FindCallable;
    using ValueHolder = typename RawImpl::ValueHolder;

    void ALWAYS_INLINE emplace(Key x, ValueHolder & it, bool & inserted) { dispatch(x, EmplaceCallable{it, inserted}); }
    void ALWAYS_INLINE emplace(Key x, ValueHolder & it, bool & inserted, DB::Arena & pool)
    {
        dispatch(x, EmplaceCallableWithPool{it, inserted, pool});
    }
    ValueHolder ALWAYS_INLINE find(Key x) { return dispatch(x, FindCallable{}); }
    ValueHolder ALWAYS_INLINE end() { return ValueHolder{}; }

    using iterator = ValueHolder;

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
