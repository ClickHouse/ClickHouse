#include <iostream>
#include <common/SimpleCache.h>


static int func(int x, int y)
{
    std::cerr << x << " + " << y << "\n";
    return x + y;
}


int main(int, char **)
{
    SimpleCache<decltype(func), &func> func_cached;

    std::cerr << func_cached(1, 2) << "\n";
    std::cerr << func_cached(1, 2) << "\n";
    std::cerr << func_cached(1, 2) << "\n";
    std::cerr << func_cached(3, 4) << "\n";
    std::cerr << func_cached(3, 4) << "\n";
    std::cerr << func_cached(3, 4) << "\n";
}
