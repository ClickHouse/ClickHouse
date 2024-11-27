#pragma once

/// Packed versions HashMap, please keep in sync with HashMap.h

#include <Common/HashTable/HashMap.h>

/// A pair that does not initialize the elements, if not needed.
///
/// NOTE: makePairNoInit() is omitted for PackedPairNoInit since it is not
/// required for PackedHashMap (see mergeBlockWithPipe() for details)
template <typename First, typename Second>
struct __attribute__((packed)) PackedPairNoInit
{
    First first;
    Second second;

    PackedPairNoInit() {} /// NOLINT

    template <typename FirstValue>
    PackedPairNoInit(FirstValue && first_, NoInitTag)
        : first(std::forward<FirstValue>(first_))
    {
    }

    template <typename FirstValue, typename SecondValue>
    PackedPairNoInit(FirstValue && first_, SecondValue && second_)
        : first(std::forward<FirstValue>(first_))
        , second(std::forward<SecondValue>(second_))
    {
    }
};

/// The difference with ZeroTraits is that PackedZeroTraits accepts PackedPairNoInit instead of Key.
namespace PackedZeroTraits
{
    template <typename First, typename Second, template <typename, typename> class PackedPairNoInit>
    bool check(const PackedPairNoInit<First, Second> p) { return p.first == First{}; }

    template <typename First, typename Second, template <typename, typename> class PackedPairNoInit>
    void set(PackedPairNoInit<First, Second> & p) { p.first = First{}; }
}

/// setZero() should be overwritten to pass the pair instead of key, to avoid
/// "reference binding to misaligned address" errors from UBsan.
template <typename Key, typename TMapped, typename Hash, typename TState = HashTableNoState>
struct PackedHashMapCell : public HashMapCell<Key, TMapped, Hash, TState, PackedPairNoInit<Key, TMapped>>
{
    using Base = HashMapCell<Key, TMapped, Hash, TState, PackedPairNoInit<Key, TMapped>>;
    using State = typename Base::State;
    using value_type = typename Base::value_type;
    using key_type = typename Base::key_type;
    using Mapped = typename Base::Mapped;

    using Base::Base;

    void setZero() { PackedZeroTraits::set(this->value); }

    Key getKey() const { return this->value.first; }
    static Key getKey(const value_type & value_) { return value_.first; }

    Mapped & getMapped() { return this->value.second; }
    Mapped getMapped() const { return this->value.second; }
    value_type getValue() const { return this->value; }

    bool keyEquals(const Key key_) const { return bitEqualsByValue(this->value.first, key_); }
    bool keyEquals(const Key key_, size_t /*hash_*/) const { return bitEqualsByValue(this->value.first, key_); }
    bool keyEquals(const Key key_, size_t /*hash_*/, const State & /*state*/) const { return bitEqualsByValue(this->value.first, key_); }

    bool isZero(const State & state) const { return isZero(this->value.first, state); }
    static bool isZero(const Key key, const State & /*state*/) { return ZeroTraits::check(key); }

    static bool bitEqualsByValue(key_type a, key_type b) { return a == b; }

    template <size_t I>
    auto get() const
    {
        if constexpr (I == 0) return this->value.first;
        else if constexpr (I == 1) return this->value.second;
    }
};

namespace std
{
    template <typename Key, typename TMapped, typename Hash, typename TState>
    struct tuple_size<PackedHashMapCell<Key, TMapped, Hash, TState>> : std::integral_constant<size_t, 2> { };

    template <typename Key, typename TMapped, typename Hash, typename TState>
    struct tuple_element<0, PackedHashMapCell<Key, TMapped, Hash, TState>> { using type = Key; };

    template <typename Key, typename TMapped, typename Hash, typename TState>
    struct tuple_element<1, PackedHashMapCell<Key, TMapped, Hash, TState>> { using type = TMapped; };
}

/// Packed HashMap - HashMap with structure without padding
///
/// Sometimes padding in structure can be crucial, consider the following
/// example <UInt64, UInt16> as <Key, Value> in this case the padding overhead
/// is 0.375, and this can be major in case of lots of keys.
///
/// Note, there is no need to provide PackedHashSet, since it cannot have padding.
template <
    typename Key,
    typename Mapped,
    typename Hash = DefaultHash<Key>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator>
using PackedHashMap = HashMapTable<Key, PackedHashMapCell<Key, Mapped, Hash, HashTableNoState>, Hash, Grower, Allocator>;
