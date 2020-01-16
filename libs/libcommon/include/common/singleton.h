#pragma once

#include <memory>
#include <type_traits>

template <class T, typename DefaultConstructable = void>
class Singleton;

/// For default-constructable type we don't need to implement |create()|
/// and may use "arrow" operator immediately.
template <class T>
class Singleton<T, std::enable_if_t<std::is_default_constructible_v<T>>>
{
public:
    T * operator->()
    {
        static T instance;
        return &instance;
    }
};

/// For custom-constructed type we have to enforce call to |create()|
/// before any use of "arrow" operator.
template <class T>
class Singleton<T, std::enable_if_t<!std::is_default_constructible_v<T>>>
{
public:
    Singleton() = default;

    template <typename ... Args>
    Singleton(const Args & ... args)
    {
        instance.reset(new T(args...));
        /// TODO: throw exception on double-creation.
    }

    T * operator->()
    {
        return instance.get();
    }

private:
    inline static std::unique_ptr<T> instance{};
};
