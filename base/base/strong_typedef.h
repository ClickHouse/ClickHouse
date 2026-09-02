#pragma once

#include <functional>
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
    template <class Enable = typename std::is_copy_constructible<T>::type>
    constexpr explicit StrongTypedef(const T & t_) : t(t_) {}
    template <class Enable = typename std::is_move_constructible<T>::type>
    constexpr explicit StrongTypedef(T && t_) : t(std::move(t_)) {}

    template <class Enable = typename std::is_default_constructible<T>::type>
    constexpr StrongTypedef(): t() {}

    constexpr StrongTypedef(const Self &) = default;
    constexpr StrongTypedef(Self &&) noexcept(std::is_nothrow_move_constructible_v<T>) = default; // NOLINT(cppcoreguidelines-noexcept-move-operations, hicpp-noexcept-move, performance-noexcept-move-constructor)

    Self & operator=(const Self &) = default;
    Self & operator=(Self &&) noexcept(std::is_nothrow_move_assignable_v<T>)= default; // NOLINT(cppcoreguidelines-noexcept-move-operations, hicpp-noexcept-move, performance-noexcept-move-constructor)

    template <class Enable = typename std::is_copy_assignable<T>::type>
    Self & operator=(const T & rhs) { t = rhs; return *this;}

    template <class Enable = typename std::is_move_assignable<T>::type>
    Self & operator=(T && rhs) { t = std::move(rhs); return *this;}

    // NOLINTBEGIN(google-explicit-constructor)
    constexpr operator const T & () const { return t; }
    operator T & () { return t; }
    // NOLINTEND(google-explicit-constructor)

    bool operator==(const Self & rhs) const { return t == rhs.t; }
    bool operator<(const Self & rhs) const { return t < rhs.t; }
    bool operator>(const Self & rhs) const { return t > rhs.t; }

    T & toUnderType() { return t; }
    const T & toUnderType() const { return t; }
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

// NOLINTBEGIN(bugprone-macro-parentheses)

#define STRONG_TYPEDEF(T, D) \
    struct D ## Tag {}; \
    using D = StrongTypedef<T, D ## Tag>; \

// NOLINTEND(bugprone-macro-parentheses)
