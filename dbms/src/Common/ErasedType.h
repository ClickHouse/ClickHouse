#pragma once
#include <memory>

namespace DB
{

struct ErasedType
{
    using Ptr = std::unique_ptr<void, std::function<void(void *)>>;

    template <typename T, typename... Args>
    static Ptr create(const Args & ... args)
    {
        return Ptr(static_cast<void *>(new T(args...)), [](void * ptr) { delete reinterpret_cast<T *>(ptr); });
    }

    template <typename T>
    static T & get(Ptr & ptr)
    {
        return *reinterpret_cast<T *>(ptr.get());
    }
};

}
