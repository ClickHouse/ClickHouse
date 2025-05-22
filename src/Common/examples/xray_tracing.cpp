#include <Common/XRayTracing.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>

#include <pcg_random.hpp>

using namespace DB;

#define ATTRIBUTES [[clang::xray_always_instrument, clang::optnone]]

template <typename T>
ALWAYS_INLINE void doWork(T arg)
{
    pcg64_fast rndgen;
    std::uniform_int_distribution<uint64_t> dist(0, 1000000);
    T sum = arg;
    for (int i = 0; i < 1000000; ++i)
        sum += dist(rndgen);
}

ATTRIBUTES void always_traced_overloaded_function()
{
    XRAY_TRACE(always_traced_overloaded_function)
    doWork(42);
}

ATTRIBUTES void always_traced_overloaded_function(int arg)
{
    void (*ptr)(int) = always_traced_overloaded_function;
    XRAY_TRACE(ptr)
    doWork(arg);
}

template <typename T>
ATTRIBUTES void always_traced_template_function(T & arg)
{
    XRAY_TRACE(always_traced_template_function<T>)
    doWork(arg);
}

template <typename T, typename U>
ATTRIBUTES void always_traced_template_function(T & arg1, U & arg2)
{
    XRAY_TRACE((always_traced_template_function<T, U>))
    doWork(arg1 + arg2);
}

class MyClass
{
public:
    virtual ~MyClass() = default;

    ATTRIBUTES
    void f(int arg) const
    {
        XRAY_TRACE(MyClass, f)
        doWork(arg);
    }

    ATTRIBUTES
    virtual void g(int arg)
    {
        XRAY_TRACE(MyClass, g)
        doWork(arg);
    }

    template <typename T>
    ATTRIBUTES void h(T arg)
    {
        XRAY_TRACE(MyClass, h<T>)
        doWork(arg);
    }

    ATTRIBUTES
    void j() const
    {
        void (MyClass::*my_ptr)() const = &MyClass::j;
        XRAY_TRACE(my_ptr)
        doWork(42);
    }

    ATTRIBUTES
    void j(int arg) const
    {
        void (MyClass::*my_ptr)(int) const = &MyClass::j;
        XRAY_TRACE(my_ptr)
        doWork(arg);
    }
};

int main()
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cout));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    __xray_init();

    {
        always_traced_overloaded_function();
        always_traced_overloaded_function();
    }

    {
        int arg = 42;
        always_traced_overloaded_function(arg);
        always_traced_overloaded_function(arg);
    }

    {
        int arg = 42;
        always_traced_template_function(arg);
        always_traced_template_function(arg);
    }

    {
        int arg1 = 42;
        double arg2 = 3.14;
        always_traced_template_function(arg1, arg2);
        always_traced_template_function(arg1, arg2);
    }

    {
        int arg = 42;
        MyClass obj;
        obj.f(arg);
        obj.f(arg);
    }

    {
        int arg = 42;
        MyClass obj;
        obj.g(arg);
        obj.g(arg);
    }

    {
        int arg = 42;
        MyClass obj;
        obj.h(arg);
        obj.h(arg);
    }

    {
        MyClass obj;
        obj.j();
        obj.j();
    }

    return 0;
}
