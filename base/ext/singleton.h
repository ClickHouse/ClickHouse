#pragma once

#include <memory>

namespace ext
{

/** Thread-unsafe singleton. It works simply like a global variable.
  * Supports deinitialization.
  *
  * In most of the cases, you don't need this class.
  * Use "Meyers Singleton" instead: static T & instance() { static T x; return x; }
  */

template <class T>
class Singleton
{
public:
    Singleton()
    {
        if (!instance)
            instance = std::make_unique<T>();
    }

    T * operator->()
    {
        return instance.get();
    }

    static bool isInitialized()
    {
        return !!instance;
    }

    static void reset()
    {
        instance.reset();
    }

private:
    inline static std::unique_ptr<T> instance{};
};

}
