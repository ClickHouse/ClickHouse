#include <iostream>
#include <sys/time.h>
#include <unordered_map>
#include <string>
#include <unordered_set>
#include <cassert>
#include <chrono>
#include "IncrementalRehashTable.h"

int64_t timeit()
{
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}
#include <list>
int main()
{
    std::cout << sizeof(my_unordered_set<std::string>) << std::endl;
    std::cout << sizeof(std::unordered_set<std::string>) << std::endl;
    std::cout << sizeof(std::unique_ptr<my_unordered_set<std::string>>) << std::endl;
    std::cout << sizeof(std::shared_ptr<my_unordered_set<std::string>>) << std::endl;
    std::cout << sizeof(std::string) << std::endl;
    return 0;
    my_unordered_set<std::string> t;
    my_unordered_map<std::string, int> rmp;
    robin_hood::unordered_map<std::string, int> mp;
    std::cout << "map is flat " << my_unordered_map<std::string, std::list<std::string>::iterator>::is_flat << std::endl;
    std::cout << "set  map is flat " << my_unordered_set<std::string>::is_flat << std::endl;
    #if 0
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
    #endif
    {
        int N = 10000000;
        auto btime = timeit();
        for (int i = 0; i < N; ++i)
            mp.insert(robin_hood::pair(std::make_pair(std::string("abcd" + std::to_string(i)), i)));
        auto etime = timeit();
        std::cout << "robin hood map use time ms " << etime - btime << ", size" << mp.size() << std::endl;
        for (int i = 0; i < N; ++i)
            assert(mp.contains(std::string("abcd") + std::to_string(i)));

        std::unordered_map<std::string, int> orimap;
        btime = timeit();
        for (int i = 0; i < N; ++i)
            orimap.insert(std::make_pair(std::string("bbbb" + std::to_string(i)), i));
        etime = timeit();
        std::cout << "std unorderedmap use time ms " << etime - btime << std::endl;

        btime = timeit();
        for (int i = 0; i < N; ++i)
            rmp.insert(robin_hood::pair(std::make_pair(std::string("cccc" + std::to_string(i)), i)));
        etime = timeit();
        std::cout << "my unorderedmap use time ms " << etime - btime << std::endl;
    }
}
