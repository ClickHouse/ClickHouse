#pragma once
#include <Core/Types.h>
#include "delta_kernel_ffi.hpp"

namespace DB
{
namespace ErrorCodes
{
    extern const int DELTA_KERNEL_ERROR;
    extern const int LOGICAL_ERROR;
}

}
namespace DeltaLake
{

struct KernelUtils
{
    static ffi::KernelStringSlice toDeltaString(const std::string & s)
    {
        return ffi::KernelStringSlice{ .ptr = s.data(), .len = s.size() };
    }

    static std::string fromDeltaString(const struct ffi::KernelStringSlice slice)
    {
        return std::string(slice.ptr, slice.len);
    }

    static void * allocateString(const struct ffi::KernelStringSlice slice)
    {
        return new std::string(slice.ptr, slice.len);
    }

    struct KernelError : ffi::EngineError
    {
        static ffi::EngineError * allocateError(ffi::KernelError etype_, ffi::KernelStringSlice msg)
        {
            auto error = new KernelError;
            error->etype = etype_;
            error->error_message = std::string(msg.ptr, msg.len);
            return error;
        }

        [[noreturn]] void rethrow(const std::string & from)
        {
            throw DB::Exception(
                DB::ErrorCodes::DELTA_KERNEL_ERROR,
                "Received DeltaLake kernel error: {} (in {})",
                error_message, from);
        }

        // The error message from Kernel
        std::string error_message;
    };

    template <class T>
    static T unwrapResult(ffi::ExternResult<T> result, const std::string & from)
    {
        if (result.tag == ffi::ExternResult<T>::Tag::Ok)
            return result.ok._0;

        if (result.tag == ffi::ExternResult<T>::Tag::Err)
        {
            if (result.err._0)
            {
                auto kernel_error = static_cast<KernelUtils::KernelError *>(result.err._0);
                kernel_error->rethrow(from);
            }
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Received DeltaLake unknown kernel error");
        }
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid error ExternResult tag found!");
    }
};

}
