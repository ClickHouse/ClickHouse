#include <iostream>
#include <string_view>
#include <dlfcn.h>
#include <Core/Defines.h>
#include <base/demangle.h>
#include <Common/Dwarf.h>
#include <Common/Elf.h>
#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>
#include <Examples/clickhouse_examples.h>

[[maybe_unused]] static NO_INLINE const void * getAddress()
{
    return __builtin_return_address(0);
}

int mainEntryExampleSymbolIndex(int argc, char ** argv)
{
#if defined(__ELF__) && !defined(OS_FREEBSD)
    using namespace DB;

    if (argc < 2)
    {
        std::cerr << "Usage: ./symbol_index address\n";
        return 1;
    }

    const SymbolIndex & symbol_index = SymbolIndex::instance();

    auto iterator = symbol_index.iterateSymbols();
    const SymbolIndex::Symbol * elem = nullptr;
    std::string_view name;
    while (iterator.next(elem, name))
        std::cout << name << ": " << elem->offset_begin << " ... " << elem->offset_end << "\n";
    std::cout << "\n";

    const void * address = reinterpret_cast<void*>(std::stoull(argv[1], nullptr, 16));

    const auto * symbol = symbol_index.findSymbol(address);
    std::string_view symbol_name = symbol ? symbol_index.getSymbolName(*symbol) : std::string_view("");
    if (!symbol_name.empty())
        std::cerr << symbol_name << ": " << symbol->offset_begin << " ... " << symbol->offset_end << "\n";
    else
        std::cerr << "SymbolIndex: Not found\n";

    Dl_info info;
    if (dladdr(address, &info) && info.dli_sname)
        std::cerr << demangle(info.dli_sname) << ": " << info.dli_saddr << "\n";
    else
        std::cerr << "dladdr: Not found\n";

    const auto * object = symbol_index.findObject(getAddress());
    Dwarf dwarf(object->elf);

    Dwarf::LocationInfo location;
    VectorWithMemoryTracking<Dwarf::SymbolizedFrame> frames;
    if (dwarf.findAddress(uintptr_t(address) - uintptr_t(info.dli_fbase), location, Dwarf::LocationInfoMode::FAST, frames))
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
