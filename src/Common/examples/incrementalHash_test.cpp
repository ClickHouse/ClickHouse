#include "../HashTable/IncrementalRehashTable.h"
//// test 
#include <sys/time.h>
#include <unordered_map>
#include <string>
#include <unordered_set>
#include <cassert>
#include <iostream>

int64_t timeit()
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

int main()
{
    my_unordered_set<std::string> t;
    my_unordered_map<std::string, int> mp;
    {
        int N = 30000000;
        auto btime = timeit();
        for (int i = 0; i < N; ++i)
            t.insert(std::string("abcd" + std::to_string(i)));
        auto etime = timeit();
        std::cout << "myset use time ms " << etime - btime << ", size" << t.size() << ", ht size: " << t.htsize()<< std::endl;
        for (int i = 0; i < N; ++i)
            assert(t.contains(std::string("abcd") + std::to_string(i)));
        std::unordered_set<std::string> oriset;
        btime = timeit();
        for (int i = 0; i < N; ++i)
            oriset.insert(std::string("bbbb" + std::to_string(i)));
        etime = timeit();
        std::cout << "std use time ms " << etime - btime << std::endl;
    }
    {
        int N = 30000000;
        auto btime = timeit();
        for (int i = 0; i < N; ++i)
            mp.insert(robin_hood::pair(std::make_pair(std::string("abcd" + std::to_string(i)), i)));
        auto etime = timeit();
        std::cout << "mymap use time ms " << etime - btime << ", size" << mp.size() << ", ht size: " << mp.htsize()<< std::endl;
        for (int i = 0; i < N; ++i)
            assert(mp.contains(std::string("abcd") + std::to_string(i)));
        std::unordered_map<std::string, int> orimap;
        btime = timeit();
        for (int i = 0; i < N; ++i)
            orimap.insert(std::make_pair(std::string("bbbb" + std::to_string(i)), i));
        etime = timeit();
        std::cout << "std unorderedmap use time ms " << etime - btime << std::endl;
    }
}
