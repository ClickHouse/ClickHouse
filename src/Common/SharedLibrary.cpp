#include "SharedLibrary.h"
#include <string>
#include <boost/core/noncopyable.hpp>
#include <common/phdr_cache.h>
#include "Exception.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_DLSYM;
}

SharedLibrary::SharedLibrary(const std::string & path, int flags)
{
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
