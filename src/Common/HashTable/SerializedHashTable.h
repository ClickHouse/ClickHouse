#pragma once

#include <Common/HashTable/HashTable.h>

struct SerializedHashTableHash
{
    size_t ALWAYS_INLINE operator()(StringRefWithInlineHash key) const { return key.size; }
    size_t ALWAYS_INLINE operator()(StringRef key) const { return StringRefHash()(key); }
};

template <typename Mapped>
struct SerializedHashTableLookupResult
{
    Mapped * mapped_ptr;
    SerializedHashTableLookupResult() { } /// NOLINT
    SerializedHashTableLookupResult(Mapped * mapped_ptr_) /// NOLINT
        : mapped_ptr(mapped_ptr_)
    {
    } /// NOLINT
    SerializedHashTableLookupResult(std::nullptr_t) { } /// NOLINT
    const VoidKey getKey() const { return {}; } /// NOLINT
    auto & getMapped() { return *mapped_ptr; }
    auto & operator*() { return *this; }
    auto & operator*() const { return *this; }
    auto * operator->() { return this; }
    auto * operator->() const { return this; }
    explicit operator bool() const { return mapped_ptr; }
    friend bool operator==(const SerializedHashTableLookupResult & a, const std::nullptr_t &) { return !a.mapped_ptr; }
    friend bool operator==(const std::nullptr_t &, const SerializedHashTableLookupResult & b) { return !b.mapped_ptr; }
    friend bool operator!=(const SerializedHashTableLookupResult & a, const std::nullptr_t &) { return a.mapped_ptr; }
    friend bool operator!=(const std::nullptr_t &, const SerializedHashTableLookupResult & b) { return b.mapped_ptr; }
};

template <typename SubMaps>
class SerializedHashTable : private boost::noncopyable
{
protected:
    // Medium-length strings (up to 65535 bytes) are stored as StringRefWithHash
    using Th = typename SubMaps::Th;

    // Long strings are stored as StringRef along with saved hash
    using Ts = typename SubMaps::Ts;
    using Self = SerializedHashTable;

    template <typename, typename, size_t>
    friend class TwoLevelSerializedHashTable;

    Th mh;
    Ts ms;

public:
    using Key = StringRef;
    using key_type = Key;
    using mapped_type = typename Ts::mapped_type;
    using value_type = typename Ts::value_type;
    using cell_type = typename Ts::cell_type;

    using LookupResult = SerializedHashTableLookupResult<typename cell_type::mapped_type>;
    using ConstLookupResult = SerializedHashTableLookupResult<const typename cell_type::mapped_type>;

    SerializedHashTable() = default;

    explicit SerializedHashTable(size_t reserve_for_num_elements)
        : mh{reserve_for_num_elements}
        // , ms{reserve_for_num_elements / 2}
    {
    }

    SerializedHashTable(SerializedHashTable && rhs) noexcept
        : mh(std::move(rhs.mh))
        , ms(std::move(rhs.ms))
    {
    }

    ~SerializedHashTable() = default;

    template <typename Self, typename KeyHolder, typename Func>
    static auto ALWAYS_INLINE dispatch(Self & self, KeyHolder && key_holder, size_t hash, Func && func)
    {
        const StringRef & x = keyHolderGetKey(key_holder);
        if (x.size <= std::numeric_limits<UInt32>::max()) [[likely]]
        {
            chassert(x.size == (hash >> StringRefWithInlineHash::SHIFT));
            StringRefWithInlineHash str_inlined_hash{x.data, hash};
            if constexpr (std::is_same_v<DB::ArenaKeyHolder, std::decay_t<KeyHolder>>)
            {
                return func(self.mh, DB::ArenaKeyHolderWithInlineHash{str_inlined_hash, key_holder.pool}, str_inlined_hash.size);
            }
            else if constexpr (std::is_same_v<DB::SerializedKeyHolder, std::decay_t<KeyHolder>>)
            {
                return func(self.mh, DB::SerializedKeyHolderWithInlineHash{str_inlined_hash, key_holder.pool}, str_inlined_hash.size);
            }
            else
            {
                return func(self.mh, str_inlined_hash, str_inlined_hash.size);
            }
        }
        else
        {
            return func(self.ms, std::forward<KeyHolder>(key_holder), hash);
        }
    }

    struct EmplaceCallable
    {
        LookupResult & mapped;
        bool & inserted;
        const std::function<void()> & resize_callback;

        EmplaceCallable(LookupResult & mapped_, bool & inserted_, const std::function<void()> & resize_callback_)
            : mapped(mapped_)
            , inserted(inserted_)
            , resize_callback(resize_callback_)
        {
        }

        template <typename Map, typename KeyHolder>
        void ALWAYS_INLINE operator()(Map & map, KeyHolder && key_holder, size_t hash)
        {
            typename Map::LookupResult result;
            map.emplace(key_holder, result, inserted, hash, resize_callback);
            mapped = &result->getMapped();
        }
    };

    template <typename KeyHolder>
    void ALWAYS_INLINE
    emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted, size_t hash, const std::function<void()> & resize_callback = {})
    {
        this->dispatch(*this, key_holder, hash, EmplaceCallable(it, inserted, resize_callback));
    }

    void ALWAYS_INLINE prefetchByHash(size_t hash, size_t len) const
    {
        if (len <= std::numeric_limits<UInt32>::max()) [[likely]]
            mh.prefetchByHash(hash);
        else
            ms.prefetchByHash(hash);
    }

    bool ALWAYS_INLINE isEmptyCell(size_t key_hash, size_t len) const
    {
        if (len <= std::numeric_limits<UInt32>::max()) [[likely]]
            return mh.isEmptyCell(key_hash);
        else
            return ms.isEmptyCell(key_hash);
    }

    struct FindCallable
    {
        // find() doesn't need any key memory management, so we don't work with
        // any key holders here, only with normal keys. The key type is still
        // different for every subtable, this is why it is a template parameter.
        template <typename Submap, typename SubmapKey>
        auto ALWAYS_INLINE operator()(Submap & map, const SubmapKey & key, size_t hash)
        {
            auto it = map.find(key, hash);
            if (!it)
                return decltype(&it->getMapped()){};
            return &it->getMapped();
        }
    };

    LookupResult ALWAYS_INLINE find(const Key & x, size_t hash) { return dispatch(*this, x, hash, FindCallable{}); }

    ConstLookupResult ALWAYS_INLINE find(const Key & x, size_t hash) const { return dispatch(*this, x, hash, FindCallable{}); }

    void write(DB::WriteBuffer & wb) const
    {
        mh.write(wb);
        ms.write(wb);
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        mh.writeText(wb);
        DB::writeChar(',', wb);
        ms.writeText(wb);
    }

    void read(DB::ReadBuffer & rb)
    {
        mh.read(rb);
        ms.read(rb);
    }

    void readText(DB::ReadBuffer & rb)
    {
        mh.readText(rb);
        DB::assertChar(',', rb);
        ms.readText(rb);
    }

    size_t size() const { return mh.size() + ms.size(); }

    bool empty() const { return mh.empty() && ms.empty(); }

    size_t getBufferSizeInBytes() const { return mh.getBufferSizeInBytes() + ms.getBufferSizeInBytes(); }

    void clearAndShrink()
    {
        mh.clearAndShrink();
        ms.clearAndShrink();
    }
};
