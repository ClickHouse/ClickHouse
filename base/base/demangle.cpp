#include <base/demangle.h>

#include <cstdlib>

#if defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)

/** `abi::__cxa_demangle` is a complete C++ name parser. In a standalone build of the parser it is
  * over 130 KB - more than `src/IO` and `src/Core` put together - and the only thing that reaches
  * it is a type name inside an exception message that this build never goes on to display.
  * Return the mangled name, which is what `demangle` already does when demangling fails.
  */

DemangleResult tryDemangle(const char *)
{
    return {};
}

std::string demangle(const char * name, int & status)
{
    status = -1;
    return name;
}

#else

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
