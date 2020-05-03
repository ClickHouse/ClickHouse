#include "SharedLibrary.h"
#include <common/phdr_cache.h>
#include <Common/config.h>
#include "Exception.h"

#if USE_DL
#include <dlfcn.h>
#else

extern "C"
{
    int RTLD_LAZY = 0;
    void * dlopen(const char *, int) { return nullptr; }
    void * dlsym(void *, const char *) { return nullptr; }
    int dlclose(void *) { return 0; }
    char * dlerror() { return const_cast<char *>("Server was build without support for dynamic library loading"); }
}

#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_DLSYM;
}

SharedLibrary::SharedLibrary(const std::string & path, int flags)
{
    if (flags == -1)
        flags = RTLD_LAZY;

    handle = dlopen(path.c_str(), flags);
    if (!handle)
        throw Exception(std::string("Cannot dlopen: ") + dlerror(), ErrorCodes::CANNOT_DLOPEN);

    updatePHDRCache();

    /// NOTE: race condition exists when loading multiple shared libraries concurrently.
    /// We don't care (or add global mutex for this method).
}

SharedLibrary::~SharedLibrary()
{
    if (handle && dlclose(handle))
        std::terminate();
}

void * SharedLibrary::getImpl(const std::string & name, bool no_throw)
{
    dlerror();

    auto * res = dlsym(handle, name.c_str());

    if (char * error = dlerror())
    {
        if (no_throw)
            return nullptr;
        throw Exception(std::string("Cannot dlsym: ") + error, ErrorCodes::CANNOT_DLSYM);
    }

    return res;
}
}
