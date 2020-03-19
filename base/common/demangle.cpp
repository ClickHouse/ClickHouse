#include "demangle.h"

#include "defines.h"


#if defined(MEMORY_SANITIZER)

DemangleResult tryDemangle(const char * name)
{
    return DemangleResult{};
}

std::string demangle(const char * name, int & status)
{
    status = 0;
    return name;
}

#else

#    include <cxxabi.h>
#    include <stdlib.h>

static DemangleResult tryDemangle(const char * name, int & status)
{
    return DemangleResult(abi::__cxa_demangle(name, nullptr, nullptr, &status));
}

DemangleResult tryDemangle(const char * name)
{
    int status = 0;
    return tryDemangle(name, status);
}

std::string demangle(const char * name, int & status)
{
    auto result = tryDemangle(name, status);
    if (result)
    {
        return std::string(result.get());
    }

    return name;
}

#endif
