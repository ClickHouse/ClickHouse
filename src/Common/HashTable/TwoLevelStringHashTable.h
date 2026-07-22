#pragma once

#include <Common/HashTable/StringHashTable.h>

template <typename SubMaps, typename ImplTable = StringHashTable<SubMaps>, size_t BITS_FOR_BUCKET = 8>
class TwoLevelStringHashTable : private boost::noncopyable
{
protected:
    using HashValue = size_t;
    using Self = TwoLevelStringHashTable;

public:
    using Key = std::string_view;
    using Impl = ImplTable;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    // TODO: currently hashing contains redundant computations when doing distributed or external aggregations
    size_t hash(const Key & x) const
    {
        return const_cast<Self &>(*this).dispatch(*this, x, [&](const auto &, const auto &, size_t hash) { return hash; });
    }

    size_t operator()(const Key & x) const { return hash(x); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Impl impls[NUM_BUCKETS];

    TwoLevelStringHashTable() = default;

    explicit TwoLevelStringHashTable(size_t size_hint)
    {
        for (auto & impl : impls)
            impl.reserve(size_hint / NUM_BUCKETS);
    }

    template <typename Source>
    explicit TwoLevelStringHashTable(const Source & src)
    {
        if (src.m0.hasZero())
            impls[0].m0.setHasZero(*src.m0.zeroValue());

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

    /// The bucket-aware size-class dispatch: the shared implementation in `StringHashTable`
    /// classifies and packs the key and computes its hash; the resolver here picks the bucket
    /// from that hash and hands out the bucket's submap of the right class.
    template <typename Self, typename Func, typename KeyHolder>
    static auto ALWAYS_INLINE dispatch(Self & self, KeyHolder && key_holder, Func && func)
    {
        return Impl::dispatchOnKeyClass(
            [&](auto key_class, size_t hash_value) -> auto &
            { return self.impls[getBucketFromHash(hash_value)].template submapForClass<decltype(key_class)::value>(); },
            std::forward<KeyHolder>(key_holder),
            StringHashTableHash{},
            std::forward<Func>(func));
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        dispatch(*this, key_holder, typename Impl::EmplaceCallable{it, inserted});
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder && key_holder)
    {
        dispatch(*this, std::forward<KeyHolder>(key_holder), typename Impl::PrefetchCallable{});
    }

    LookupResult ALWAYS_INLINE find(const Key x)
    {
        return dispatch(*this, x, typename Impl::FindCallable{});
    }

    ConstLookupResult ALWAYS_INLINE find(const Key x) const
    {
        return dispatch(*this, x, typename Impl::FindCallable{});
    }

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

    size_t size() const
    {
        size_t res = 0;
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].size();

        return res;
    }

    bool empty() const
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            if (!impls[i].empty())
                return false;

        return true;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t res = 0;
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].getBufferSizeInBytes();

        return res;
    }
};
