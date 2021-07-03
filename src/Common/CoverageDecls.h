#pragma once

/// FreeBSD and Darwin do not have DWARF, so coverage build is explicitly disabled.
/// Fake classes are introduced to be able to build CH.

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

namespace coverage
{
    struct SymbolIndexInstance {};
    struct SymbolIndex { static constexpr SymbolIndexInstance instance() { return {}; } };

    struct Dwarf
    {
        struct File { std::string toString() const { return {}; } }; //NOLINT
        struct LocationInfo { size_t line; File file; };

        constexpr LocationInfo findAddressForCoverageRuntime(uintptr_t) const { return {}; } //NOLINT
    };
}

#endif

namespace coverage
{

enum class Magic : uint32_t
{
    ReportHeader = 0xcafefefe,
    TestEntry = 0xcafecafe
};

class FileWrapper
{
    FILE * handle {nullptr};

    inline void write(uint32_t s) { fwrite(&s, sizeof(uint32_t), 1, handle); }

public:
    inline FILE * set(const std::string& pathname, const char * mode)
    {
        handle = fopen(pathname.data(), mode);
        return handle;
    }

    inline void write(Magic m) { write(static_cast<uint32_t>(m)); }
    inline void write(size_t s) { write(static_cast<uint32_t>(s)); }
    inline void write(int s) { write(static_cast<uint32_t>(s)); }

    inline void write(const String& str)
    {
        write(str.size());
        fwrite(str.c_str(), str.size(), 1, handle);
    }

    inline void close() { fclose(handle); handle = nullptr; }
};
}
