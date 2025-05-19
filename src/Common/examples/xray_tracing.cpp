#include <Common/XRayTracing.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>

#include <pcg_random.hpp>

using namespace DB;

template <typename T>
ALWAYS_INLINE void doWork(T arg)
{
    pcg64_fast rndgen;
    std::uniform_int_distribution<uint64_t> dist(0, 1000000);
    T sum = arg;
    for (int i = 0; i < 1000000; ++i)
        sum += dist(rndgen);
    LOG_DEBUG(&Poco::Logger::get("debug"), "sum={}", sum);
}

[[clang::xray_always_instrument]]
void always_traced_function()
{
    OMG(always_traced_function)
    doWork(42);
}

template <typename T>
[[clang::xray_always_instrument]]
void always_traced_template_function(T & arg)
{
    OMG(always_traced_template_function<T>)
    doWork(arg);
}

template <typename T, typename U>
[[clang::xray_always_instrument]]
void always_traced_template_function(T & arg1, U & arg2)
{
    OMG((always_traced_template_function<T, U>))
    doWork(arg1 + arg2);
}

class MyClass
{
public:
    virtual ~MyClass() = default;

    [[clang::xray_always_instrument]]
    void f(int arg)
    {
        OMG_MEMBER(MyClass, f)
        doWork(arg);
    }

    [[clang::xray_always_instrument]]
    virtual void g(int arg)
    {
        OMG_VIRT_MEMBER(MyClass, g)
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
        always_traced_function();
        always_traced_function();
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

    return 0;
}
