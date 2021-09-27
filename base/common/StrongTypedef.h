#pragma once

#include <type_traits>
#include <utility>

/**
 * Type that defeats implicit C++ conversions.
 * @example StrongTypedefInt a; a = 0 won't compile.
 */
template <class T, class Tag>
struct StrongTypedef
{
private:
    using Self = StrongTypedef;
    T t;

    static constexpr bool trivial = std::is_trivially_constructible_v<T>;

public:
    using UnderlyingType = T;

    constexpr explicit StrongTypedef(const T & t_)
        requires(std::is_copy_constructible_v<T> && !trivial)
        : t(t_) {}

    constexpr explicit StrongTypedef(T && t_)
        requires(std::is_move_constructible_v<T> && !trivial)
        : t(std::move(t_)) {}

    constexpr explicit StrongTypedef(T t_)
        requires(trivial)
        : t(t_) {}

    constexpr StrongTypedef()
        requires(std::is_default_constructible_v<T>)
        : t() {}

    constexpr StrongTypedef(const Self &) = default;
    constexpr StrongTypedef(Self &&) = default;

    constexpr StrongTypedef & operator=(const Self &) = default;
    constexpr StrongTypedef & operator=(Self &&) = default;

    //NOLINTNEXTLINE Allow implicit conversions to underlying type, you would need to change a lot of code otherwise
    constexpr operator const T & () const { return t; }
    constexpr operator T & () { return t; } //NOLINT

    // How great would the world be if we could just use <=> or =default.
    constexpr bool operator<(const Self& other) const { return t < other.t; }
    constexpr bool operator>(const Self& other) const { return t > other.t; }
    constexpr bool operator==(const Self& other) const { return t == other.t; }
    constexpr bool operator!=(const Self& other) const { return t != other.t; }
    constexpr bool operator>=(const Self& other) const { return t >= other.t; }
    constexpr bool operator<=(const Self& other) const { return t <= other.t; }

    constexpr T & toUnderType() { return t; }
    constexpr const T & toUnderType() const { return t; }
};

namespace std
{
    template <class T, class Tag>
    struct hash<StrongTypedef<T, Tag>>
    {
        size_t operator()(const StrongTypedef<T, Tag> & x) const
        {
            return std::hash<T>()(x.toUnderType());
        }
    };
}
