#pragma once

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
struct DemangleResult
{
    char * data = nullptr;
    size_t size = 0;

    DemangleResult() = default;
    DemangleResult(DemangleResult &) = delete;
    DemangleResult(DemangleResult && other)
    {
        std::swap(data, other.data);
        std::swap(size, other.size);
    }
    ~DemangleResult()
    {
        if (data)
        {
            free(data);
        }
    }
};

DemangleResult try_demangle(const char * name);
