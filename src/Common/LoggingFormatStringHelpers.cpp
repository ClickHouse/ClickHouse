#include <stdexcept>

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error)
{
    throw std::runtime_error(error);
}
