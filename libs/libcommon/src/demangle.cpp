#include <common/demangle.h>

#if defined(__has_feature)
    #if __has_feature(memory_sanitizer)
        #define MEMORY_SANITIZER 1
    #endif
#elif defined(__MEMORY_SANITIZER__)
    #define MEMORY_SANITIZER 1
#endif

#if _MSC_VER || MEMORY_SANITIZER

std::string demangle(const char * name, int & status)
{
    status = 0;
    return name;
}

#else

#include <stdlib.h>
#include <cxxabi.h>

static DemangleResult try_demangle(const char * name, int & status)
{
    DemangleResult result;
    result.data = abi::__cxa_demangle(name, nullptr, &result.size, &status);
    return result;
}

DemangleResult try_demangle(const char * name)
{
    int status = 0;
    return try_demangle(name, status);
}

std::string demangle(const char * name, int & status)
{
    auto result = try_demangle(name, status);
    if (result.data)
    {
        return std::string(result.data, result.size - 1);
    }

    return name;
}


#endif
