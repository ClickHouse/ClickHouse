#pragma once

#include <cstdlib>
#include <memory>
#include <string>


/** Demangles C++ symbol name.
  * When demangling fails, returns the original name and sets status to non-zero.
  * TODO: Write msvc version (now returns the same string)
  */
std::string demangle(const char * name, int & status);

inline std::string demangle(const char * name)
{
    int status = 0;
    return demangle(name, status);
}

// abi::__cxa_demangle returns a C string of known size that should be deleted
// with free().
struct FreeingDeleter
{
    template <typename PointerType>
    void operator() (PointerType ptr)
    {
        std::free(ptr);
    }
};

using DemangleResult = std::unique_ptr<char, FreeingDeleter>;

DemangleResult tryDemangle(const char * name);
