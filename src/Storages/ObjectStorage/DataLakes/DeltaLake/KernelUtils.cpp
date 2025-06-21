#include "KernelUtils.h"

#if USE_DELTA_KERNEL_RS
#include "delta_kernel_ffi.hpp"
#include <fmt/ranges.h>

namespace DB::ErrorCodes
{
    extern const int DELTA_KERNEL_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace DeltaLake
{

ffi::KernelStringSlice KernelUtils::toDeltaString(const std::string & string)
{
    return ffi::KernelStringSlice{ .ptr = string.data(), .len = string.size() };
}

std::string KernelUtils::fromDeltaString(ffi::KernelStringSlice slice)
{
    return std::string(slice.ptr, slice.len);
}

void * KernelUtils::allocateString(ffi::KernelStringSlice slice)
{
    return new std::string(slice.ptr, slice.len);
}

namespace
{
struct KernelError : public ffi::EngineError
{
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

[[noreturn]] void KernelUtils::throwError(ffi::EngineError * error, const std::string & from)
{
    auto * kernel_error = static_cast<KernelError *>(error);
    auto error_message_copy = kernel_error->error_message;
    auto etype_copy = kernel_error->etype;
    delete kernel_error;

    throw DB::Exception(
        DB::ErrorCodes::DELTA_KERNEL_ERROR,
        "Received DeltaLake kernel error {}: {} (in {})",
        etype_copy, error_message_copy, from);
}

std::string getPhysicalName(const std::string & name, const DB::NameToNameMap & physical_names_map)
{
    if (physical_names_map.empty())
        return name;

    auto it = physical_names_map.find(name);
    if (it == physical_names_map.end())
    {
        DB::Names keys;
        keys.reserve(physical_names_map.size());
        for (const auto & [key, _] : physical_names_map)
            keys.push_back(key);

        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Not found column {} in physical names map. There are only columns: {}",
            name, fmt::join(keys, ", "));
    }
    return it->second;
}

}

#endif
