#pragma once

#include <memory>

namespace ext {

template <class T>
class Singleton
{
public:
    Singleton()
    {
        if (!instance)
            instance.reset(new T);
    }

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

    static bool isInitialized() {
        return !!instance;
    }

private:
    inline static std::unique_ptr<T> instance{};
};

}
