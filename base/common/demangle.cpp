#include <common/demangle.h>

#if defined(__has_feature)
    #if __has_feature(memory_sanitizer)
        #define MEMORY_SANITIZER 1
    #endif
#elif defined(__MEMORY_SANITIZER__)
    #define MEMORY_SANITIZER 1
#endif

#if _MSC_VER || MEMORY_SANITIZER

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

#include <stdlib.h>
#include <cxxabi.h>

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
