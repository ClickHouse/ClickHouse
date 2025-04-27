#pragma once

#include <Common/HashTable/SerializedHashTable.h>

template <typename SubMaps, typename ImplTable = SerializedHashTable<SubMaps>, size_t BITS_FOR_BUCKET = 8>
class TwoLevelSerializedHashTable : private boost::noncopyable
{
protected:
    using HashValue = size_t;
    using Self = TwoLevelSerializedHashTable;

public:
    using Key = StringRef;
    using Impl = ImplTable;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Impl impls[NUM_BUCKETS];

    TwoLevelSerializedHashTable() = default;
    TwoLevelSerializedHashTable(size_t) { } /// NOLINT

    explicit TwoLevelSerializedHashTable(const Impl & src)
    {
        for (auto & v : src.mh)
        {
            size_t hash_value = v.getHash(src.mh);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].mh.insertUniqueNonZero(&v, hash_value);
        }
        for (auto & v : src.ms)
        {
            size_t hash_value = v.getHash(src.ms);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].ms.insertUniqueNonZero(&v, hash_value);
        }
    }

    // This function is mostly the same as SerializedHashTable::dispatch, but with
    // added bucket computation. See the comments there.
    template <typename Self, typename Func, typename KeyHolder>
    static auto ALWAYS_INLINE dispatch(Self & self, KeyHolder && key_holder, size_t hash, Func && func)
    {
        const StringRef & x = keyHolderGetKey(key_holder);
        if (x.size <= std::numeric_limits<UInt32>::max()) [[likely]]
        {
            chassert(x.size == (hash >> StringRefWithInlineHash::SHIFT));
            StringRefWithInlineHash str_inlined_hash{x.data, hash};
            auto buck = getBucketFromHash(str_inlined_hash.size);
            if constexpr (std::is_same_v<DB::ArenaKeyHolder, std::decay_t<KeyHolder>>)
            {
                return func(
                    self.impls[buck].mh, DB::ArenaKeyHolderWithInlineHash{str_inlined_hash, key_holder.pool}, str_inlined_hash.size);
            }
            else if constexpr (std::is_same_v<DB::SerializedKeyHolder, std::decay_t<KeyHolder>>)
            {
                return func(
                    self.impls[buck].mh, DB::SerializedKeyHolderWithInlineHash{str_inlined_hash, key_holder.pool}, str_inlined_hash.size);
            }
            else
            {
                return func(self.impls[buck].mh, str_inlined_hash, str_inlined_hash.size);
            }
        }
        else
        {
            auto buck = getBucketFromHash(hash);
            return func(self.impls[buck].ms, std::forward<KeyHolder>(key_holder), hash);
        }
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE
    emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash, const std::function<void()> & resize_callback = {})
    {
        dispatch(*this, key_holder, hash, typename Impl::EmplaceCallable{it, inserted, resize_callback});
    }

    void ALWAYS_INLINE prefetchByHash(size_t hash, size_t len) const
    {
        if (len <= std::numeric_limits<UInt32>::max()) [[likely]]
            impls[getBucketFromHash(hash)].mh.prefetchByHash(hash);
        else
            impls[getBucketFromHash(hash)].ms.prefetchByHash(hash);
    }

    bool ALWAYS_INLINE isEmptyCell(size_t key_hash, size_t len) const
    {
        if (len <= std::numeric_limits<UInt32>::max()) [[likely]]
            impls[getBucketFromHash(key_hash)].mh.isEmptyCell(key_hash, len);
        else
            impls[getBucketFromHash(key_hash)].ms.isEmptyCell(key_hash, len);
    }

    LookupResult ALWAYS_INLINE find(const Key x, size_t hash) { return dispatch(*this, x, hash, typename Impl::FindCallable{}); }

    ConstLookupResult ALWAYS_INLINE find(const Key x, size_t hash) const { return dispatch(*this, x, hash, typename Impl::FindCallable{}); }

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
