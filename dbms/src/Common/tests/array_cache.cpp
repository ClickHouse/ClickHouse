#include <iostream>
#include <Common/ArrayCache.h>


int main(int argc, char ** argv)
{
    ArrayCache<int, int> cache(1024 * 1024 * 1024);

    return 0;
}
