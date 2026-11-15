#pragma once

#include <Common/HashTable/HashTableAllocator.h>

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <utility>

#if defined(__aarch64__)
#include <arm_neon.h>
#elif defined(__SSE2__)
#include <emmintrin.h>
#endif

/// An experimental read index over the ordinary table's cells and collision chains.
/// Build, payload construction, iteration and offsets retain the underlying table's contract.
/// `prepareFingerprints` seals a snapshot after build; mutation invalidates that snapshot.
template <typename BaseTable, bool simd, typename MetadataAllocator = HashTableAllocator>
class FingerprintHashTable : public BaseTable
{
    using Key = typename BaseTable::key_type;
    using Cell = typename BaseTable::cell_type;
    static_assert(std::is_same_v<Key, UInt64>);

    struct MetadataDeleter
    {
        size_t bytes = 0;

        void operator()(uint8_t * data) const
        {
            MetadataAllocator().free(data, bytes);
        }
    };

    std::unique_ptr<uint8_t, MetadataDeleter> metadata{nullptr, MetadataDeleter{}};
    mutable std::atomic<const uint8_t *> active_metadata{nullptr};
    static constexpr size_t width = 16;

    void invalidateFingerprints()
    {
        if (metadata)
        {
            active_metadata.store(nullptr, std::memory_order_relaxed);
            metadata.reset();
        }
    }

    static uint8_t fingerprint(size_t hash)
    {
        /// Hardware CRC has 32 meaningful bits. Its upper byte is independent of small-table bucket bits.
        /// Arm SIMD preserves all nonzero tag bytes; scalar and SSE2 retain the original low-bit mapping.
        /// Equality always checks the full key.
#if defined(__aarch64__)
        if constexpr (simd)
        {
            const uint8_t tag = static_cast<uint8_t>(hash >> 24);
            return tag == 0 ? 1 : tag;
        }
#endif
        return static_cast<uint8_t>((hash >> 24) | 1);
    }

    static unsigned equalMask(const uint8_t * data, uint8_t needle)
    {
#if defined(__aarch64__)
        const auto matches = vceqq_u8(vld1q_u8(data), vdupq_n_u8(needle));
        static constexpr uint8_t weights[width] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
        const auto bits = vandq_u8(matches, vld1q_u8(weights));
        return unsigned(vaddv_u8(vget_low_u8(bits))) | (unsigned(vaddv_u8(vget_high_u8(bits))) << 8);
#elif defined(__SSE2__)
        return static_cast<unsigned>(_mm_movemask_epi8(
            _mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(data)), _mm_set1_epi8(static_cast<char>(needle)))));
#else
        unsigned mask = 0;
        for (size_t i = 0; i < width; ++i)
            mask |= unsigned(data[i] == needle) << i;
        return mask;
#endif
    }

    typename BaseTable::ConstLookupResult findPrepared(const Key & key, size_t hash, const uint8_t * index) const
    {
        if (key == 0)
            return this->hasZero() ? this->zeroValue() : nullptr;

        const size_t mask = this->grower.bufSize() - 1;
        size_t position = this->grower.place(hash);
        const uint8_t tag = fingerprint(hash);
        if constexpr (simd)
        {
            while (true)
            {
                const auto * group = index + position;
#if defined(__aarch64__)
                const auto bytes = vld1q_u8(group);
                const auto equal = vceqq_u8(bytes, vdupq_n_u8(tag));
                if (vmaxvq_u8(equal) == 0)
                {
                    /// A missing tag still stops at the first empty slot; only a full group can advance.
                    if (vminvq_u8(bytes) == 0)
                        return nullptr;
                    position = (position + width) & mask;
                    continue;
                }
#endif
                const unsigned empty = equalMask(group, 0);
                unsigned matches = equalMask(group, tag);
                if (empty)
                    matches &= (1U << std::countr_zero(empty)) - 1;
                while (matches)
                {
                    const size_t candidate = (position + std::countr_zero(matches)) & mask;
                    if (this->buf[candidate].keyEquals(key, hash, *this))
                        return &this->buf[candidate];
                    matches &= matches - 1;
                }
                if (empty)
                    return nullptr;
                position = (position + width) & mask;
            }
        }
        else
        {
            while (index[position])
            {
                if (index[position] == tag && this->buf[position].keyEquals(key, hash, *this))
                    return &this->buf[position];
                position = (position + 1) & mask;
            }
            return nullptr;
        }
    }

public:
    using BaseTable::BaseTable;
    using LookupResult = typename BaseTable::LookupResult;
    using ConstLookupResult = typename BaseTable::ConstLookupResult;
    static constexpr bool uses_simd = simd;

    FingerprintHashTable() = default;

    /// All allocation precedes the nonthrowing ownership transfer. Nonzero cell addresses and all offsets survive.
    /// The embedded zero cell moves before any probe handle may escape.
    explicit FingerprintHashTable(BaseTable & source)
    {
        static_assert(std::is_nothrow_move_assignable_v<BaseTable>);
        const size_t capacity = source.getBufferSizeInCells();
        chassert(capacity >= width);
        const size_t bytes = capacity + width - 1;
        std::unique_ptr<uint8_t, MetadataDeleter> next(
            static_cast<uint8_t *>(MetadataAllocator().alloc(bytes)), MetadataDeleter{bytes});
        std::memset(next.get(), 0, bytes);
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            const auto * cell = it.getPtr();
            if (const size_t offset = source.offsetInternal(cell))
                next.get()[offset - 1] = fingerprint(source.hash(cell->getKey()));
        }
        std::memcpy(next.get() + capacity, next.get(), width - 1);
        static_cast<BaseTable &>(*this) = std::move(source);
        metadata = std::move(next);
        active_metadata.store(metadata.get(), std::memory_order_relaxed);
    }

    FingerprintHashTable(FingerprintHashTable && other) noexcept
        : BaseTable(std::move(other))
        , metadata(std::move(other.metadata))
        , active_metadata(other.active_metadata.exchange(nullptr, std::memory_order_relaxed))
    {
    }

    FingerprintHashTable & operator=(FingerprintHashTable && other) noexcept
    {
        if (this != &other)
        {
            BaseTable::operator=(std::move(other));
            metadata = std::move(other.metadata);
            active_metadata.store(other.active_metadata.exchange(nullptr, std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }

    bool disableFingerprints() const
    {
        /// Readers may finish an indexed lookup. Keep its allocation alive until table destruction.
        return active_metadata.exchange(nullptr, std::memory_order_relaxed) != nullptr;
    }

    bool fingerprintsEnabled() const
    {
        return active_metadata.load(std::memory_order_relaxed) != nullptr;
    }

    ConstLookupResult findInBase(const Key & key) const
    {
        return BaseTable::find(key);
    }

    void prepareFingerprints()
    {
        const size_t capacity = this->grower.bufSize();
        chassert(capacity >= width);
        const size_t bytes = capacity + width - 1;
        std::unique_ptr<uint8_t, MetadataDeleter> next(
            static_cast<uint8_t *>(MetadataAllocator().alloc(bytes)), MetadataDeleter{bytes});
        for (size_t i = 0; i < capacity; ++i)
            next.get()[i] = this->buf[i].isZero(*this) ? 0 : fingerprint(this->hash(this->buf[i].getKey()));
        std::memcpy(next.get() + capacity, next.get(), width - 1);
        metadata.swap(next);
        active_metadata.store(metadata.get(), std::memory_order_relaxed);
    }

    size_t fingerprintBytes() const
    {
        return metadata ? metadata.get_deleter().bytes : 0;
    }

    size_t getBufferSizeInBytes() const
    {
        return BaseTable::getBufferSizeInBytes() + fingerprintBytes();
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & key, size_t hash) const
    {
        /// Lookups during incremental build use the ordinary table, before a read index exists.
        const auto * index = active_metadata.load(std::memory_order_relaxed);
        if (!index)
            return BaseTable::find(key, hash);
        return findPrepared(key, hash, index);
    }

    ConstLookupResult ALWAYS_INLINE find(const Key & key) const
    {
        return find(key, this->hash(key));
    }

    LookupResult ALWAYS_INLINE find(const Key & key, size_t hash)
    {
        return const_cast<Cell *>(std::as_const(*this).find(key, hash));
    }

    LookupResult ALWAYS_INLINE find(const Key & key)
    {
        return find(key, this->hash(key));
    }

    bool has(const Key & key) const
    {
        return find(key) != nullptr;
    }
    bool has(const Key & key, size_t hash) const
    {
        return find(key, hash) != nullptr;
    }
    bool contains(const Key & key) const
    {
        return has(key);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder && holder) const
    {
        const auto hash = this->hash(keyHolderGetKey(holder));
        prefetchByHash(hash);
        keyHolderDiscardKey(holder);
    }

    void ALWAYS_INLINE prefetchByHash(size_t hash) const
    {
        if (const auto * index = active_metadata.load(std::memory_order_relaxed))
            __builtin_prefetch(index + this->grower.place(hash));
        else
            BaseTable::prefetchByHash(hash);
    }

    template <typename... Args>
    void ALWAYS_INLINE emplace(Args &&... args)
    {
        invalidateFingerprints();
        BaseTable::emplace(std::forward<Args>(args)...);
    }

    template <typename... Args>
    auto insert(Args &&... args)
    {
        invalidateFingerprints();
        return BaseTable::insert(std::forward<Args>(args)...);
    }

    template <typename... Args>
    auto erase(Args &&... args)
    {
        invalidateFingerprints();
        return BaseTable::erase(std::forward<Args>(args)...);
    }

    void reserve(size_t count)
    {
        invalidateFingerprints();
        BaseTable::reserve(count);
    }

    void clear()
    {
        invalidateFingerprints();
        BaseTable::clear();
    }

    void clearAndShrink()
    {
        invalidateFingerprints();
        BaseTable::clearAndShrink();
    }

    void read(DB::ReadBuffer & buffer)
    {
        invalidateFingerprints();
        BaseTable::read(buffer);
    }

    void readText(DB::ReadBuffer & buffer)
    {
        invalidateFingerprints();
        BaseTable::readText(buffer);
    }
};

namespace DB
{
template <typename...> struct HasConstructorOfNumberOfElements;

template <typename Base, bool simd, typename Allocator>
struct HasConstructorOfNumberOfElements<FingerprintHashTable<Base, simd, Allocator>>
    : HasConstructorOfNumberOfElements<Base>
{
};
}
