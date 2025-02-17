#include "KernelUtils.h"

#if USE_DELTA_KERNEL_RS
#include "delta_kernel_ffi.hpp"

namespace DB::ErrorCodes
{
    extern const int DELTA_KERNEL_ERROR;
}

namespace DeltaLake
{

ffi::KernelStringSlice KernelUtils::toDeltaString(const std::string & string)
{
    return ffi::KernelStringSlice{ .ptr = string.data(), .len = string.size() };
}

std::string KernelUtils::fromDeltaString(const struct ffi::KernelStringSlice slice)
{
    return std::string(slice.ptr, slice.len);
}

void * KernelUtils::allocateString(const struct ffi::KernelStringSlice slice)
{
    return new std::string(slice.ptr, slice.len);
}

namespace
{
struct KernelError : public ffi::EngineError
{
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
}

ffi::EngineError * KernelUtils::allocateError(ffi::KernelError etype, ffi::KernelStringSlice message)
{
    auto * error = new KernelError;
    error->etype = etype;
    error->error_message = std::string(message.ptr, message.len);
    return error;
}

[[noreturn]] void KernelUtils::rethrow(ffi::EngineError * error, const std::string & from)
{
    auto * kernel_error = static_cast<KernelError *>(error);
    kernel_error->rethrow(from);
}

}

#endif
