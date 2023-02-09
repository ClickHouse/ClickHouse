#include "SharedLibrary.h"
#include <string>
#include <boost/core/noncopyable.hpp>
#include <base/phdr_cache.h>
#include "Exception.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_DLSYM;
}

SharedLibrary::SharedLibrary(std::string_view path, int flags)
{
    handle = dlopen(path.data(), flags);
    if (!handle)
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot dlopen: ({})", dlerror());

    updatePHDRCache();

    /// NOTE: race condition exists when loading multiple shared libraries concurrently.
    /// We don't care (or add global mutex for this method).
}

SharedLibrary::~SharedLibrary()
{
    if (handle && dlclose(handle))
        std::terminate();
}

void * SharedLibrary::getImpl(std::string_view name, bool no_throw)
{
    dlerror();

    auto * res = dlsym(handle, name.data());

    if (char * error = dlerror())
    {
        if (no_throw)
            return nullptr;

        throw Exception(ErrorCodes::CANNOT_DLSYM, "Cannot dlsym: ({})", error);
    }

    return res;
}
}
