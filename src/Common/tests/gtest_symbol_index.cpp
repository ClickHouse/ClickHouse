#include <gtest/gtest.h>

#if defined(__ELF__) && !defined(OS_FREEBSD)

#include <dlfcn.h>
#include <Common/SymbolIndex.h>

using namespace DB;

namespace
{
/// A function defined in the main binary, used to obtain the main binary's load base.
void mainBinaryMarker()
{
}
}

/// Regression test for wrong shared library symbol resolution in stack traces.
///
/// `SymbolIndex` used to store file-relative symbol offsets (`st_value`) for all loaded
/// objects in a single flat vector sorted by offset. File offset ranges overlap between
/// the main binary and shared libraries, so a lookup of a shared library address could
/// return a symbol from the wrong object. `SymbolIndex` now stores absolute virtual
/// addresses (`info->addr + st_value`), so a shared library address resolves to the
/// symbol that actually contains it.
TEST(SymbolIndex, ResolveSharedLibrarySymbol)
{
    const SymbolIndex & symbol_index = SymbolIndex::instance();

    /// Resolve the real runtime address of a C library function. Using `dlsym` instead of
    /// taking the address directly avoids getting a PLT stub from the main binary.
    void * func_address = dlsym(RTLD_DEFAULT, "getpid");
    ASSERT_NE(func_address, nullptr);

    Dl_info info{};
    ASSERT_NE(dladdr(func_address, &info), 0);
    ASSERT_NE(info.dli_sname, nullptr);

    /// Skip on fully static builds (for example, musl), where the C library is part of the
    /// main binary and there is no separate object whose offsets could be mixed up.
    Dl_info self_info{};
    ASSERT_NE(dladdr(reinterpret_cast<const void *>(&mainBinaryMarker), &self_info), 0);
    if (info.dli_fbase == self_info.dli_fbase)
        GTEST_SKIP() << "The C library is statically linked into the main binary";

    const auto * symbol = symbol_index.findSymbol(func_address);
    ASSERT_NE(symbol, nullptr);

    /// The address must lie within the resolved symbol's absolute range ...
    EXPECT_LE(symbol->offset_begin, func_address);
    EXPECT_LT(func_address, symbol->offset_end);

    /// ... and that symbol must start exactly at the C library symbol reported by the
    /// dynamic linker, i.e. it comes from the shared library and not from an overlapping
    /// main binary symbol.
    EXPECT_EQ(symbol->offset_begin, info.dli_saddr);
}

#endif
