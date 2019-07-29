#include <Common/SymbolIndex.h>
#include <Common/Elf.h>
#include <Common/Dwarf.h>
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

    for (const auto & elem : symbol_index.objects())
        std::cout << elem.name << ": " << elem.address_begin << " ... " << elem.address_end << "\n";

    const void * address = reinterpret_cast<void*>(std::stoull(argv[1], nullptr, 16));

    auto symbol = symbol_index.findSymbol(address);
    if (symbol)
        std::cerr << symbol->name << ": " << symbol->address_begin << " ... " << symbol->address_end << "\n";
    else
        std::cerr << "SymbolIndex: Not found\n";

    Dl_info info;
    if (dladdr(address, &info) && info.dli_sname)
        std::cerr << demangle(info.dli_sname) << ": " << info.dli_saddr << "\n";
    else
        std::cerr << "dladdr: Not found\n";

    Elf elf("/proc/self/exe");
    Dwarf dwarf(elf);

    Dwarf::LocationInfo location;
    if (dwarf.findAddress(uintptr_t(address), location, Dwarf::LocationInfoMode::FULL))
        std::cerr << location.file.toString() << ":" << location.line << "\n";
    else
        std::cerr << "Dwarf: Not found\n";

    return 0;
}
