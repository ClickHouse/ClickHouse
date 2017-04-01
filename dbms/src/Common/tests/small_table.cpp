#include <iostream>
#include <iomanip>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/SmallTable.h>



int main(int argc, char ** argv)
{
    {
        using Cont = SmallSet<int, 16>;
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
        using Cont = SmallMap<int, std::string, 16>;
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
        using Cont = SmallSet<DB::UInt128, 16>;
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
