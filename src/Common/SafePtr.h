#pragma once

#include <memory>
#include <type_traits>
#include <utility>

#include <Common/Exception.h>

namespace DB
{

template <typename T, typename Deleter = std::default_delete<T>>
class SafeUniquePtr : public std::unique_ptr<T, Deleter> {
private:
    using Base = std::unique_ptr<T, Deleter>;

    inline void assertNotNull() const
    {
#ifndef NDEBUG
        if (!*this)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dereferencing null pointer");
        }
#endif
    }

public:
    using Base::Base;

    /// Additional constructors for compatibility
    SafeUniquePtr() noexcept : Base() {}
    explicit SafeUniquePtr(T * p) noexcept : Base(p) {}
    SafeUniquePtr(std::nullptr_t) noexcept : Base(nullptr) {}
    SafeUniquePtr(SafeUniquePtr && other) noexcept : Base(std::move(other)) {}
    SafeUniquePtr(Base && other) noexcept : Base(std::move(other)) {}

    template <typename U, typename E,
              typename = std::enable_if_t<std::is_convertible_v<U *, T *> && std::is_convertible_v<E, Deleter>>>
    SafeUniquePtr(SafeUniquePtr<U, E> && other) noexcept
        : Base(std::move(static_cast<std::unique_ptr<U, E> &>(other)))
    {}

    using Base::operator=;

    SafeUniquePtr & operator=(SafeUniquePtr && other) noexcept
    {
        Base::operator=(std::move(other));
        return *this;
    }

    T & operator*() const
    {
        assertNotNull();
        return Base::operator*();
    }

    T * operator->() const
    {
        assertNotNull();
        return Base::operator->();
    }
};


}
