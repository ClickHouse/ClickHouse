#include "foundationdb/fdb_c.h"
#include <exception>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <dlfcn.h>
#include <incbin.h>
#include <unistd.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ZstdInflatingReadBuffer.h>
#include <IO/copyData.h>

#if __has_feature(memory_sanitizer)
#    include <sanitizer/msan_interface.h>
#    define REAL(func) reinterpret_cast<decltype(&func)>(dlsym(RTLD_NEXT, #func))

extern "C" {
int dladdr1(const void * __address, Dl_info * __info, void ** __extra_info, int __flags)
{
    auto res = REAL(dladdr1)(__address, __info, __extra_info, __flags);
    if (res != 0)
    {
        __msan_unpoison(__info, sizeof(Dl_info));
        __msan_unpoison_string(__info->dli_fname);
    }
    return res;
}
}
#endif

INCBIN(fdb_c_library, FDB_C_RESOURCE_PATH);

namespace
{
class FDBLibrary
{
public:
    static FDBLibrary instance;

    ~FDBLibrary()
    {
        // FIXME: dlcose return is ignored
        if (handle)
            dlclose(handle);
    }

    // Load libfdb_c.so to /tmp dir. Temp file will be deleted automatically.
    void loadToTemp();

    // Load libfdb_.so to /tmp
    void loadToPath(const char * path);

private:
    void * handle = nullptr;

    /// Load fdb apis from handle
    void loadApis();
};

FDBLibrary FDBLibrary::instance;

void fdb_unavailable()
{
    throw std::runtime_error("fdb api is unavailable");
}

void FDBLibrary::loadToTemp()
{
    if (handle)
        return;

#ifdef __linux__
    char filename[] = "/tmp/libfdb_c.so.XXXXXXX";
    int fd = mkstemp(filename);
    if (fd == -1)
        throw std::system_error(errno, std::generic_category(), "Failed to create tmp file");
    if (close(fd) == -1)
        throw std::system_error(errno, std::generic_category(), "Failed to close tmp file");

    loadToPath(filename);

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    unlink(filename);
#else
    throw std::runtime_error("Loading fdb_c library on non-linux system is not support");
#endif
}

void FDBLibrary::loadToPath(const char * path)
{
    if (handle)
        return;

    {
        if (gfdb_c_librarySize == 0)
            throw std::runtime_error("fdb is not loaded: libfdb_c.so is not embeded");

        DB::WriteBufferFromFile wb(path);
        DB::ZstdInflatingReadBuffer rb(
            std::make_unique<DB::ReadBufferFromMemory>(reinterpret_cast<const char *>(gfdb_c_libraryData), gfdb_c_librarySize));
        DB::copyData(rb, wb);
    }

    handle = dlopen(path, RTLD_LAZY);
    if (!handle)
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        throw std::runtime_error(std::string("fdb_c: failed to dlopen ") + dlerror());

    loadApis();
}

void FDBLibrary::loadApis()
{
    if (!handle)
        throw std::runtime_error("dl is not opened");

    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    dlerror();
    void * symbol = nullptr;

    // NOLINTBEGIN(bugprone-macro-parentheses, concurrency-mt-unsafe)
#define M(name) \
    symbol = dlsym(handle, #name); \
    if (char * error = dlerror()) \
        throw std::runtime_error("failed to dlsym " #name); \
    else \
        name = reinterpret_cast<FDB_C_API_TYPE(name)>(symbol);
    APPLY_FDB_C_APIS(M)
#undef M
    // NOLINTEND(bugprone-macro-parentheses, concurrency-mt-unsafe)
}
}

void fdb_load_library()
{
    FDBLibrary::instance.loadToTemp();
}

void fdb_load_library(const std::string & path)
{
    if (path.empty())
        FDBLibrary::instance.loadToTemp();
    else
        FDBLibrary::instance.loadToPath(path.c_str());
}

// NOLINTBEGIN(bugprone-macro-parentheses)
#define M(name) FDB_C_API_TYPE(name) name = reinterpret_cast<FDB_C_API_TYPE(name)>(&fdb_unavailable);
APPLY_FDB_C_APIS(M)
#undef M
// NOLINTEND(bugprone-macro-parentheses)
