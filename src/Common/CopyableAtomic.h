#pragma once

#include <atomic>
#include <utility>

namespace DB
{

template <typename T>
struct CopyableAtomic
{
    CopyableAtomic(const CopyableAtomic & other)
        : value(other.value.load())
    {}

    template <std::convertible_to<T> U>
    explicit CopyableAtomic(U && value_)
        : value(std::forward<U>(value_))
    {}

    CopyableAtomic & operator=(const CopyableAtomic & other)
    {
        value = other.value.load();
        return *this;
    }

    template <std::convertible_to<T> U>
    CopyableAtomic & operator=(U && value_)
    {
        value = std::forward<U>(value_);
        return *this;
    }

    explicit operator T() const { return value; }

    const T & getValue() const { return value; }

    std::atomic<T> value;
};

}
