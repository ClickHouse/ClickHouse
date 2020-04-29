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

SharedLibraryFactory & SharedLibraryFactory::instance()
{
    static SharedLibraryFactory ret;
    return ret;
}


SharedLibraryPtr SharedLibraryFactory::get(const std::string & path, int flags)
{
    std::lock_guard lock(libraries_mutex);
    if (!libraries.count(path))
        libraries[path] = std::make_shared<SharedLibrary>(path, flags);

    return libraries[path].get();
}


bool SharedLibraryFactory::tryUnload(const std::string & path)
{
    std::lock_guard lock(libraries_mutex);
    if (libraries.count(path))
    {
        libraries.erase(path);
        return true;
    }
    return false;
}

void SharedLibraryFactory::unloadAll()
{
    std::lock_guard lock(libraries_mutex);
    libraries.clear();
}

}
