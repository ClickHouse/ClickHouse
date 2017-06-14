#include <iostream>
#include <iomanip>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>



int main(int argc, char ** argv)
{
    {
        using Cont = HashSet<int, DefaultHash<int>, HashTableGrower<1> >;
        Cont cont;

        cont.insert(1);
        cont.insert(2);

        Cont::iterator it;
        bool inserted;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << *it << std::endl;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << *it << std::endl;

        for (auto x : cont)
            std::cerr << x << std::endl;

        std::string dump;
        {
            DB::WriteBufferFromString wb(dump);
            cont.writeText(wb);
        }

        std::cerr << "dump: " << dump << std::endl;
    }

    {
        using Cont = HashMap<int, std::string, DefaultHash<int>, HashTableGrower<1> >;
        Cont cont;

        cont.insert(Cont::value_type(1, "Hello, world!"));
        cont[1] = "Goodbye.";

        for (auto x : cont)
            std::cerr << x.first << " -> " << x.second << std::endl;

        std::string dump;
        {
            DB::WriteBufferFromString wb(dump);
            cont.writeText(wb);
        }

        std::cerr << "dump: " << dump << std::endl;
    }

    {
        using Cont = HashSet<
            DB::UInt128,
            DB::UInt128TrivialHash>;
        Cont cont;

        std::string dump;
        {
            DB::WriteBufferFromString wb(dump);
            cont.write(wb);
        }

        std::cerr << "dump: " << dump << std::endl;
    }

    return 0;
}
