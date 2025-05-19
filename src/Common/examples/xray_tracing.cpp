#include <Common/XRayTracing.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>

#include <cstdint>
#include <iostream>
#include <string>

using namespace DB;

[[clang::xray_always_instrument]]
void always_traced_function()
{
    OMG(always_traced_function)
    LOG_DEBUG(&Poco::Logger::get("debug"), "This function is always traced");
    uint64_t start = time(nullptr);
    for (int i = 0; i < 1000000; ++i)
    {
        start += i;
    }
    LOG_DEBUG(&Poco::Logger::get("debug"), "start={}", start);
}

template <typename T>
[[clang::xray_always_instrument]]
void always_traced_template_function(T & arg)
{
    OMG(always_traced_template_function<T>)
    LOG_DEBUG(&Poco::Logger::get("debug"), "This template function is always traced with arg={}", arg);
    uint64_t start = time(nullptr);
    for (int i = 0; i < 1000000; ++i)
    {
        start += i;
    }
    LOG_DEBUG(&Poco::Logger::get("debug"), "start={}", start);
}

template <typename T, typename U>
[[clang::xray_always_instrument]]
void always_traced_template_function(T & arg1, U & arg2)
{
    OMG((always_traced_template_function<T, U>))
    LOG_DEBUG(&Poco::Logger::get("debug"), "={}", reinterpret_cast<const void *>(&always_traced_template_function<T, U>));
    LOG_DEBUG(&Poco::Logger::get("debug"), "This template function is always traced with arg1={} and arg2={}", arg1, arg2);
    uint64_t start = time(nullptr);
    for (int i = 0; i < 1000000; ++i)
    {
        start += i;
    }
    LOG_DEBUG(&Poco::Logger::get("debug"), "start={}", start);
}

class MyClass
{
public:
    virtual ~MyClass() = default;

    [[clang::xray_always_instrument]]
    void f(int arg)
    {
        OMG_MEMBER(MyClass, f)
        LOG_DEBUG(&Poco::Logger::get("debug"), "This template function is always traced with arg={}", arg);
        uint64_t start = time(nullptr);
        for (int i = 0; i < 1000000; ++i)
        {
            start += i;
        }
        LOG_DEBUG(&Poco::Logger::get("debug"), "start={}", start);
    }

    [[clang::xray_always_instrument]]
    virtual void g(int arg)
    {
        OMG_VIRT_MEMBER(MyClass, g)
        LOG_DEBUG(&Poco::Logger::get("debug"), "This template function is always traced with arg={}", arg);
        uint64_t start = time(nullptr);
        for (int i = 0; i < 1000000; ++i)
        {
            start += i;
        }
        LOG_DEBUG(&Poco::Logger::get("debug"), "start={}", start);
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
