#pragma once
#include "config.h"

#if USE_DELTA_KERNEL_RS
#include <Core/Types.h>
#include <Common/Exception.h>
#include "delta_kernel_ffi.hpp"

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DeltaLake
{

/**
 * Helper methods for use with delta-kernel-rs.
 */
struct KernelUtils
{
    /// Conversions functions to convert DeltaKernel string
    /// to std::string and vica versa.
    static ffi::KernelStringSlice toDeltaString(const std::string & string);
    static std::string fromDeltaString(ffi::KernelStringSlice slice);

    /// Allocation helpers, passed to DeltaKernel.
    /// DeltaKernel would use these functions to do the allocations.
    /// We would be responsible for deallocations as well, not the library.
    static void * allocateString(ffi::KernelStringSlice slice);
    static ffi::EngineError * allocateError(ffi::KernelError etype, ffi::KernelStringSlice message);

    /// Process DeltaKernel result in cases is is ffi::ExternResult,
    /// which means that it would either contain the result of type `T` or the error.
    template <class T>
    static T unwrapResult(ffi::ExternResult<T> result, const std::string & from)
    {
        if (result.tag == ffi::ExternResult<T>::Tag::Ok)
            return result.ok._0;

        if (result.tag == ffi::ExternResult<T>::Tag::Err)
        {
            if (result.err._0)
                rethrow(result.err._0, from);

            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Received DeltaLake unknown kernel error");
        }
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Invalid error ExternResult tag found!");
    }

private:
    [[noreturn]] static void rethrow(ffi::EngineError * error, const std::string & from);
};

}

#endif
