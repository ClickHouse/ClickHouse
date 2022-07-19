#pragma once

#include <cstdint>
#include <type_traits>

// Wrapper around enum that can have multiple values (or none) set at once.
template <typename EnumTypeT, typename StorageTypeT = std::uint64_t>
struct MultiEnum
{
    using StorageType = StorageTypeT;
    using EnumType = EnumTypeT;

    MultiEnum() = default;

    template <typename ... EnumValues, typename = std::enable_if_t<std::conjunction_v<std::is_same<EnumTypeT, EnumValues>...>>>
    constexpr explicit MultiEnum(EnumValues ... v)
        : MultiEnum((toBitFlag(v) | ... | 0u))
    {}

    template <typename ValueType, typename = std::enable_if_t<std::is_convertible_v<ValueType, StorageType>>>
    constexpr explicit MultiEnum(ValueType v)
        : bitset(v)
    {
        static_assert(std::is_unsigned_v<ValueType>);
        static_assert(std::is_unsigned_v<StorageType> && std::is_integral_v<StorageType>);
    }

    MultiEnum(const MultiEnum & other) = default;
    MultiEnum & operator=(const MultiEnum & other) = default;

    bool isSet(EnumType value) const
    {
        return bitset & toBitFlag(value);
    }

    void set(EnumType value)
    {
        bitset |= toBitFlag(value);
    }

    void unSet(EnumType value)
    {
        bitset &= ~(toBitFlag(value));
    }

    void reset()
    {
        bitset = 0;
    }

    StorageType getValue() const
    {
        return bitset;
    }

    template <typename ValueType, typename = std::enable_if_t<std::is_convertible_v<ValueType, StorageType>>>
    void setValue(ValueType new_value)
    {
        // Can't set value from any enum avoid confusion
        static_assert(!std::is_enum_v<ValueType>);
        bitset = new_value;
    }

    bool operator==(const MultiEnum & other) const
    {
        return bitset == other.bitset;
    }

    template <typename ValueType, typename = std::enable_if_t<std::is_convertible_v<ValueType, StorageType>>>
    bool operator==(ValueType other) const
    {
        // Shouldn't be comparable with any enum to avoid confusion
        static_assert(!std::is_enum_v<ValueType>);
        return bitset == other;
    }

    template <typename U>
    bool operator!=(U && other) const
    {
        return !(*this == other);
    }

    template <typename ValueType, typename = std::enable_if_t<std::is_convertible_v<ValueType, StorageType>>>
    friend bool operator==(ValueType left, MultiEnum right)
    {
        return right.operator==(left);
    }

    template <typename L, typename = typename std::enable_if<!std::is_same_v<L, MultiEnum>>::type>
    friend bool operator!=(L left, MultiEnum right)
    {
        return !(right.operator==(left));
    }

private:
    StorageType bitset = 0;

    static constexpr StorageType toBitFlag(EnumType v) { return StorageType{1} << static_cast<StorageType>(v); }
};
