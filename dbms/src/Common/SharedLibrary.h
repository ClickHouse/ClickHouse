#pragma once

#include <boost/core/noncopyable.hpp>

#include <dlfcn.h>

#include <string>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_DLSYM;
}


/** Allows you to open a dynamic library and get a pointer to a function from it.
  */
class SharedLibrary : private boost::noncopyable
{
public:
    SharedLibrary(const std::string & path)
    {
        handle = dlopen(path.c_str(), RTLD_LAZY);
        if (!handle)
            throw Exception(std::string("Cannot dlopen: ") + dlerror(), ErrorCodes::CANNOT_DLOPEN);
    }

    ~SharedLibrary()
    {
        if (handle && dlclose(handle))
            std::terminate();
    }

    template <typename Func>
    Func get(const std::string & name, bool no_throw = false)
    {
        dlerror();

        Func res = reinterpret_cast<Func>(dlsym(handle, name.c_str()));

        if (char * error = dlerror())
        {
            if (no_throw)
                return nullptr;
            throw Exception(std::string("Cannot dlsym: ") + error, ErrorCodes::CANNOT_DLSYM);
        }

        return res;
    }

private:
    void * handle;
};

using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;
}
