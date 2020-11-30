#include <iostream>
#include <boost/context/fiber.hpp>

int main(int, char **)
{
    namespace ctx=boost::context;
    int a;
    ctx::fiber source{[&a](ctx::fiber&& sink)
    {
        a=0;
        int b=1;
        while (true)
        {
            sink=std::move(sink).resume();
            int next=a+b;
            a=b;
            b=next;
        }
        return std::move(sink);
    }};
    for (int j=0;j<10;++j)
    {
        source=std::move(source).resume();
        std::cout << a << " ";
    }
}
