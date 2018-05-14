#pragma once

#include <string>


/** Demangles C++ symbol name.
  * When demangling fails, returns the original name and sets status to non-zero.
  */
std::string demangle(const char * name, int & status);

inline std::string demangle(const char * name)
{
    int status = 0;
    return demangle(name, status);
}
