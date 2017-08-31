#pragma once

#include <memory>
#include <string>
#include <boost/core/noncopyable.hpp>

namespace DB
{
/** Allows you to open a dynamic library and get a pointer to a function from it.
  */
class SharedLibrary : private boost::noncopyable
{
public:
    SharedLibrary(const std::string & path);

    ~SharedLibrary();

    void * getImpl(const std::string & name, bool no_throw);

    template <typename Func>
    Func get(const std::string & name, bool no_throw = false)
    {
        return reinterpret_cast<Func>(getImpl(name, no_throw));
    }

private:
    void * handle = nullptr;
};

using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;
}
