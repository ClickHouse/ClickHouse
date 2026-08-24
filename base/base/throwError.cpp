#include <base/throwError.h>
#include <stdexcept>


[[noreturn]] void throwError(const char * err)
{
    throw std::runtime_error(err);
}
