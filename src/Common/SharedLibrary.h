#pragma once

#include <dlfcn.h>
#include <memory>
#include <string>
#include <boost/noncopyable.hpp>


namespace DB
{

/** Allows you to open a dynamic library and get a pointer to a function from it.
  */
class SharedLibrary : private boost::noncopyable
{
public:
    explicit SharedLibrary(const std::string_view & path, int flags = RTLD_LAZY);

    ~SharedLibrary();

    template <typename Func>
    Func get(const std::string_view & name)
    {
        return reinterpret_cast<Func>(getImpl(name));
    }
    template <typename Func>
    Func tryGet(const std::string_view & name)
    {
        return reinterpret_cast<Func>(getImpl(name, true));
    }

private:
    void * getImpl(const std::string_view & name, bool no_throw = false);

    void * handle = nullptr;
};

using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;

}
