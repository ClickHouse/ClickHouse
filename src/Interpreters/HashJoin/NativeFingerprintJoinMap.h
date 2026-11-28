#pragma once

#include <Common/HashTable/FingerprintHashTable.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/TwoLevelHashTable.h>

template <typename Mapped, bool simd>
struct NativeFingerprintJoinTypes
{
    using Hash = HashCRC32<UInt64>;
    using Grower = HashTableGrowerWithPrecalculation<>;
    using Cell = std::conditional_t<std::is_same_v<Mapped, VoidMapped>, HashTableCell<UInt64, Hash>, HashMapCell<UInt64, Mapped, Hash>>;
    using Table = std::conditional_t<
        std::is_same_v<Mapped, VoidMapped>,
        HashSetTable<UInt64, Cell, Hash, Grower, HashTableAllocator>,
        HashMapTable<UInt64, Cell, Hash, Grower, HashTableAllocator>>;
    using Impl = FingerprintHashTable<Table, simd>;
    using Base = TwoLevelHashTable<UInt64, Cell, Hash, Grower, HashTableAllocator, Impl, 0>;
};

template <typename Mapped, bool simd>
class NativeFingerprintJoinMap : public NativeFingerprintJoinTypes<Mapped, simd>::Base
{
    using Base = typename NativeFingerprintJoinTypes<Mapped, simd>::Base;
    using Impl = typename NativeFingerprintJoinTypes<Mapped, simd>::Impl;
    using Table = typename NativeFingerprintJoinTypes<Mapped, simd>::Table;

public:
    using Base::Base;
    static constexpr bool uses_simd = simd;

    NativeFingerprintJoinMap() = default;

    template <typename Source>
    explicit NativeFingerprintJoinMap(Source & source)
    {
        static_assert(std::extent_v<decltype(source.impls)> == 1);
        static_assert(std::is_same_v<std::remove_reference_t<decltype(source.impls[0])>, Table>);
        static_assert(std::is_nothrow_move_assignable_v<Impl>);
        /// Allocate the outer prefix before `Impl` transfers the source cell allocation.
        /// One-bucket `offsetInternal` delegates directly to the child, including zero offset 0.
        this->bucket_cells_prefix.assign(1, 0);
        Impl prepared(source.impls[0]);
        this->impls[0] = std::move(prepared);
    }

    [[nodiscard]] bool fingerprintsEnabled() const
    {
        return this->impls[0].fingerprintsEnabled();
    }

    [[nodiscard]] bool disableFingerprints() const
    {
        return this->impls[0].disableFingerprints();
    }

    typename Base::ConstLookupResult findInBase(const UInt64 & key) const
    {
        return this->impls[0].findInBase(key);
    }

    void prepareFingerprints()
    {
        this->impls[0].prepareFingerprints();
    }

    [[nodiscard]] size_t fingerprintBytes() const
    {
        return this->impls[0].fingerprintBytes();
    }

    template <typename Func>
    void forEachMapped(Func && func)
    {
        this->impls[0].forEachMapped(std::forward<Func>(func));
    }

    template <typename Func>
    void forEachValue(Func && func)
    {
        this->impls[0].forEachValue(std::forward<Func>(func));
    }
};
