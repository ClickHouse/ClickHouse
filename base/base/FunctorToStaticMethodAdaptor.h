#include <functional>

/** Adapt functor to static method where functor passed as context.
  * Main use case to convert lambda into function that can be passed into JIT code.
  */
template <typename Functor>
class FunctorToStaticMethodAdaptor : public FunctorToStaticMethodAdaptor<decltype(&Functor::operator())>
{
};

template <typename R, typename C, typename ...Args>
class FunctorToStaticMethodAdaptor<R (C::*)(Args...) const>
{
public:
    static R call(C * ptr, Args &&... arguments)
    {
        return std::invoke(&C::operator(), ptr, std::forward<Args>(arguments)...);
    }

    static R unsafeCall(char * ptr, Args &&... arguments)
    {
        C * ptr_typed = reinterpret_cast<C*>(ptr);
        return std::invoke(&C::operator(), ptr_typed, std::forward<Args>(arguments)...);
    }
};

template <typename R, typename C, typename ...Args>
class FunctorToStaticMethodAdaptor<R (C::*)(Args...)>
{
public:
    static R call(C * ptr, Args &&... arguments)
    {
        return std::invoke(&C::operator(), ptr, std::forward<Args>(arguments)...);
    }

    static R unsafeCall(char * ptr, Args &&... arguments)
    {
        C * ptr_typed = static_cast<C*>(ptr);
        return std::invoke(&C::operator(), ptr_typed, std::forward<Args>(arguments)...);
    }
};
