#include <iostream>
/// #define BOOST_USE_UCONTEXT
#include <Common/Fiber.h>
// #include <boost/context/pooled_fixedsize_stack.hpp>
// #include <boost/context/segmented_stack.hpp>
#include <Common/Exception.h>
#include <Common/FiberStack.h>

void __attribute__((__noinline__)) foo(std::exception_ptr exception)
{
    if (exception)
        std::rethrow_exception(exception);
}

void __attribute__((__noinline__)) bar(int a)
{
    std::cout << StackTrace().toString() << std::endl;

    if (a > 0)
        throw DB::Exception(0, "hello");
}

void __attribute__((__noinline__)) gar(int a)
{
    char buf[1024];
    buf[1023] = a & 255;
    if (a > 2)
        return gar(a - 1);
    else
        bar(a);
}

int main(int, char **)
try {
    namespace ctx=boost::context;
    int a;
    std::exception_ptr exception;
    // ctx::protected_fixedsize allocator
    // ctx::pooled_fixedsize_stack(1024 * 64 + 2 * 2 * 1024 * 1024 * 16, 1)
    ctx::fiber source{std::allocator_arg_t(), FiberStack(), [&](ctx::fiber&& sink)
    {
        a=0;
        int b=1;
        for (size_t i = 0; i < 9; ++i)
        {
            sink=std::move(sink).resume();
            int next=a+b;
            a=b;
            b=next;
        }
        try
        {
            gar(1024);
        }
        catch (...)
        {
            std::cout << "Saving exception\n";
            exception = std::current_exception();
        }
        return std::move(sink);
    }};

    for (int j=0;j<10;++j)
    {
        try
        {
            source=std::move(source).resume();
        }
        catch (DB::Exception & e)
        {
            std::cout << "Caught exception in resume " << e.getStackTraceString() << std::endl;
        }
        std::cout << a << " ";
    }

    std::cout << std::endl;

    try
    {
        foo(exception);
    }
    catch (const DB::Exception & e)
    {
        std::cout << e.getStackTraceString() << std::endl;
    }
}
catch (...)
{
    std::cerr << "Uncaught exception\n";
}
