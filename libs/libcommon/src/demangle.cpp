#include <common/demangle.h>
#include <stdlib.h>
#if !_MSC_VER
#include <cxxabi.h>
#endif


std::string demangle(const char * name, int & status)
{
    std::string res;

#if !_MSC_VER
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
#endif

    return res;
}
