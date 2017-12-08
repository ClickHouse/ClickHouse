#include <Common/demangle.h>
#include <cxxabi.h>
#include <stdlib.h>


std::string demangle(const char * name, int & status)
{
    std::string res;

    char * demangled_str = abi::__cxa_demangle(name, 0, 0, &status);
    if (demangled_str)
    {
        try
        {
            res = demangled_str;
        }
        catch (...)
        {
            free(demangled_str);
            throw;
        }
        free(demangled_str);
    }
    else
        res = name;

    return res;
}
