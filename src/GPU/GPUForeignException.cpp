#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Common/Exception.h>

#include <cxa_exception.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

bool dropCaughtExceptionDestructor()
{
    __cxxabiv1::__cxa_eh_globals * globals = __cxxabiv1::__cxa_get_globals();
    __cxxabiv1::__cxa_exception * caught = globals->caughtExceptions;
    if (!caught)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No exception is being handled to drop the destructor of");

    const uint64_t exception_class = caught->unwindHeader.exception_class;
    if ((exception_class & __cxxabiv1::get_vendor_and_language) != (__cxxabiv1::kOurExceptionClass & __cxxabiv1::get_vendor_and_language))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The exception being handled was not thrown through libc++abi");

    __cxxabiv1::__cxa_exception * primary = caught;
    if (exception_class == __cxxabiv1::kOurDependentExceptionClass)
        primary = static_cast<__cxxabiv1::__cxa_exception *>(
                      reinterpret_cast<__cxxabiv1::__cxa_dependent_exception *>(caught)->primaryException)
            - 1;

    primary->exceptionDestructor = nullptr;
    return primary == caught && primary->referenceCount == 1;
}

}

#endif
