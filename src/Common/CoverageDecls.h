#pragma once

#if defined(__ELF__) && !defined(__FreeBSD__)
    #define NON_ELF_BUILD 0
    #include <Common/SymbolIndex.h>
    #include <Common/Dwarf.h>
namespace coverage
{
    using SymbolIndex = DB::SymbolIndex;
    using SymbolIndexInstance = decltype(SymbolIndex::instance());
    using Dwarf = DB::Dwarf;
}
#else
    #define NON_ELF_BUILD 1
    #if WITH_COVERAGE
        #error "Coverage build does not work on FreeBSD and Darwin".
    #endif
/// FreeBSD and Darwin do not have DWARF, so coverage build is explicitly disabled.
/// Fake classes are introduced to be able to build CH.
namespace coverage
{
    struct SymbolIndexInstance {};
    struct SymbolIndex { static constexpr SymbolIndexInstance instance() { return {}; } };
    struct File { std::string toString() const { return {}; } }; //NOLINT

    struct Dwarf
    {
        struct LocationInfo { size_t line; File file; };
        constexpr LocationInfo findAddressForCoverageRuntime(uintptr_t) const { return {}; } //NOLINT
    };
}
#endif
