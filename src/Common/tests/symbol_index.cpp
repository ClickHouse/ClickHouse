#include <Common/SymbolIndex.h>
#include <Common/Elf.h>
#include <Common/Dwarf.h>
#include <Core/Defines.h>
#include <common/demangle.h>
#include <iostream>
#include <dlfcn.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
static NO_INLINE const void * getAddress()
{
    return __builtin_return_address(0);
}
#pragma GCC diagnostic pop

int main(int argc, char ** argv)
{
#if defined(__ELF__) && !defined(__FreeBSD__)
    using namespace DB;

    if (argc < 2)
    {
        std::cerr << "Usage: ./symbol_index address\n";
        return 1;
    }

    auto symbol_index_ptr = SymbolIndex::instance();
    const SymbolIndex & symbol_index = *symbol_index_ptr;

    for (const auto & elem : symbol_index.symbols())
        std::cout << elem.name << ": " << elem.address_begin << " ... " << elem.address_end << "\n";
    std::cout << "\n";

    const void * address = reinterpret_cast<void*>(std::stoull(argv[1], nullptr, 16));

    const auto * symbol = symbol_index.findSymbol(address);
    if (symbol)
        std::cerr << symbol->name << ": " << symbol->address_begin << " ... " << symbol->address_end << "\n";
    else
        std::cerr << "SymbolIndex: Not found\n";

    Dl_info info;
    if (dladdr(address, &info) && info.dli_sname)
        std::cerr << demangle(info.dli_sname) << ": " << info.dli_saddr << "\n";
    else
        std::cerr << "dladdr: Not found\n";

    const auto * object = symbol_index.findObject(getAddress());
    Dwarf dwarf(*object->elf);

    Dwarf::LocationInfo location;
    if (dwarf.findAddress(uintptr_t(address) - uintptr_t(info.dli_fbase), location, Dwarf::LocationInfoMode::FAST))
        std::cerr << location.file.toString() << ":" << location.line << "\n";
    else
        std::cerr << "Dwarf: Not found\n";

    std::cerr << "\n";
    std::cerr << StackTrace().toString() << "\n";
#else
    (void)argc;
    (void)argv;

    std::cerr << "This test does not make sense for non-ELF objects.\n";
#endif

    return 0;
}
