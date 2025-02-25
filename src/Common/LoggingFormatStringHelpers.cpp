#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error)
{
    throw std::runtime_error(error);
}
