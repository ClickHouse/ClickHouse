#pragma once

#include <type_traits>
#include <utility>

template <typename T, typename Tag>
struct StrongTypedef
{
private:
    using Self = StrongTypedef;
    T t;

public:
    using UnderlyingType = T;

    //NOLINTNEXTLINE Want to be able to initialize StrongTypedef<String> from std::string_view.
    constexpr StrongTypedef(const T & t_)
        requires(std::is_copy_constructible_v<T> && !std::is_trivially_constructible_v<T>)
        : t(t_) {}

    constexpr explicit StrongTypedef(T && t_)
        requires(std::is_move_constructible_v<T> && !std::is_trivially_constructible_v<T>)
        : t(std::move(t_)) {}

    constexpr explicit StrongTypedef(T t_)
        requires(std::is_trivially_constructible_v<T>)
        : t(t_) {}

    constexpr StrongTypedef()
        requires(std::is_default_constructible_v<T>)
        : t() {}

    constexpr StrongTypedef(const Self &) = default;
    constexpr StrongTypedef(Self &&) = default;

    constexpr StrongTypedef & operator=(const Self &) = default;
    constexpr StrongTypedef & operator=(Self &&) = default;

    constexpr StrongTypedef & operator=(const T & rhs)
        requires(std::is_copy_assignable_v<T> && !std::is_trivially_copy_assignable_v<T>)
    {
        t = rhs;
        return *this;
    }

    constexpr StrongTypedef & operator=(T && rhs)
        requires(std::is_move_assignable_v<T> && !std::is_trivially_copy_assignable_v<T>)
    {
        t = std::move(rhs);
        return *this;
    }

    constexpr StrongTypedef & operator=(T rhs)
        requires(std::is_trivially_copy_assignable_v<T>)
    {
        t = rhs;
        return *this;
    }

    constexpr operator const T & () const { return t; }
    constexpr operator T & () { return t; }

    constexpr bool operator==(const Self & rhs) const { return t == rhs.t; }
    constexpr bool operator<(const Self & rhs) const { return t < rhs.t; }
    constexpr bool operator>(const Self & rhs) const { return t > rhs.t; }

    constexpr T & toUnderType() { return t; }
    constexpr const T & toUnderType() const { return t; }
};

namespace std
{
    template <typename T, typename Tag>
    struct hash<StrongTypedef<T, Tag>>
    {
        size_t operator()(const StrongTypedef<T, Tag> & x) const
        {
            return std::hash<T>()(x.toUnderType());
        }
    };
}
