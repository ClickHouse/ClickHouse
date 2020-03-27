#include <iostream>
#include <iomanip>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>


int main(int, char **)
{
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1>>;
        Cont cont;

        cont.insert(1);
        cont.insert(2);

        Cont::LookupResult it;
        bool inserted;
        int key = 3;
        cont.emplace(key, it, inserted);
        std::cerr << inserted << ", " << key << std::endl;

        cont.emplace(key, it, inserted);
        std::cerr << inserted << ", " << key << std::endl;

        for (auto x : cont)
            std::cerr << x.getValue() << std::endl;

        DB::WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    {
        using Cont = HashSet<
            DB::UInt128,
            DB::UInt128TrivialHash>;
        Cont cont;

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    return 0;
}
