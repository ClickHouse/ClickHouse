#include <iostream>
#include <iomanip>

#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/SmallTable.h>


int main(int, char **)
{
    {
        using Cont = SmallSet<int, 16>;
        Cont cont;

        cont.insert(1);
        cont.insert(2);

        Cont::iterator it;
        bool inserted;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << it->getValue() << std::endl;

        cont.emplace(3, it, inserted);
        std::cerr << inserted << ", " << it->getValue() << std::endl;

        for (auto x : cont)
            std::cerr << x.getValue() << std::endl;

        DB::WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    {
        using Cont = SmallMap<int, std::string, 16>;
        Cont cont;

        cont.insert(Cont::value_type(1, "Hello, world!"));
        cont[1] = "Goodbye.";

        for (auto x : cont)
            std::cerr << x.getKey() << " -> " << x.getMapped() << std::endl;

        DB::WriteBufferFromOwnString wb;
        cont.writeText(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    {
        using Cont = SmallSet<DB::UInt128, 16>;
        Cont cont;

        DB::WriteBufferFromOwnString wb;
        cont.write(wb);

        std::cerr << "dump: " << wb.str() << std::endl;
    }

    return 0;
}
