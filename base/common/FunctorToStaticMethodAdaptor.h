#include <functional>

template <typename Functor>
class FunctorToStaticMethodAdaptor : public FunctorToStaticMethodAdaptor<decltype(&Functor::operator())>
{
public:
};

template <typename R, typename C, typename ...Args>
class FunctorToStaticMethodAdaptor<R (C::*)(Args...) const>
{
public:
    static R call(C * ptr, Args... arguments)
    {
        return std::invoke(&C::operator(), ptr, arguments...);
    }

    static R unsafeCall(char * ptr, Args... arguments)
    {
        C * ptr_typed = reinterpret_cast<C*>(ptr);
        return std::invoke(&C::operator(), ptr_typed, arguments...);
    }
};

template <typename R, typename C, typename ...Args>
class FunctorToStaticMethodAdaptor<R (C::*)(Args...)>
{
public:
    static R call(C * ptr, Args... arguments)
    {
        return std::invoke(&C::operator(), ptr, arguments...);
    }

    static R unsafeCall(char * ptr, Args... arguments)
    {
        C * ptr_typed = static_cast<C*>(ptr);
        return std::invoke(&C::operator(), ptr_typed, arguments...);
    }
};
