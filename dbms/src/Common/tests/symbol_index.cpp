#include <Common/SymbolIndex.h>
#include <common/demangle.h>
#include <iostream>
#include <dlfcn.h>


void f() {}

using namespace DB;

int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cerr << "Usage: ./symbol_index address\n";
        return 1;
    }

    SymbolIndex symbol_index;

    for (const auto & symbol : symbol_index)
        std::cout << symbol.name << ": " << symbol.address_begin << " ... " << symbol.address_end << "\n";

    const void * address = reinterpret_cast<void*>(std::stoull(argv[1], nullptr, 16));

    auto symbol = symbol_index.find(address);
    if (symbol)
        std::cerr << symbol->name << ": " << symbol->address_begin << " ... " << symbol->address_end << "\n";
    else
        std::cerr << "Not found\n";

    Dl_info info;
    if (dladdr(address, &info) && info.dli_sname)
        std::cerr << demangle(info.dli_sname) << ": " << info.dli_saddr << "\n";
    else
        std::cerr << "Not found\n";

    return 0;
}
