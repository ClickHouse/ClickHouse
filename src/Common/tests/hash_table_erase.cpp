#include <iomanip>
#include <iostream>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>


int main(int, char **)
{
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;
        Cont cont;

        for (size_t i = 0; i < 5000; ++i)
        {
            cont.insert(i);
        }

        for (size_t i = 0; i < 2500; ++i)
        {
            cont.erase(i);
        }

        for (size_t i = 5000; i < 10000; ++i)
        {
            cont.insert(i);
        }

        for (size_t i = 5000; i < 10000; ++i)
        {
            cont.erase(i);
        }

        for (size_t i = 2500; i < 5000; ++i)
        {
            cont.erase(i);
        }

        std::cerr << "size: " << cont.size() << std::endl;
    }

    return 0;
}
