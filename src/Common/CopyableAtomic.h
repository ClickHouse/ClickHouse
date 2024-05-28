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

    explicit CopyableAtomic(T && value_)
        : value(std::forward<T>(value_))
    {}

    CopyableAtomic & operator=(const CopyableAtomic & other)
    {
        value = other.value.load();
        return *this;
    }

    CopyableAtomic & operator=(bool value_)
    {
        value = value_;
        return *this;
    }

    explicit operator T() const { return value; }

    const T & getValue() const { return value; }

    std::atomic<T> value;
};

}
