#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#include <Common/logger_useful.h>

#include <base/defines.h>

#include <magic_enum.hpp>

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

std::mutex mutex;
std::unordered_set<uintptr_t> allocated_errors;

uintptr_t ptrToInt(void * error)
{
    return reinterpret_cast<uintptr_t>(error);
}

void recordKernelErrorAllocation(void * error)
{
    std::lock_guard guard(mutex);
    bool inserted = allocated_errors.insert(ptrToInt(error)).second;
    chassert(inserted);
}

void forgetKernelErrorAllocation(void * error)
{
    std::lock_guard guard(mutex);
    bool erased = allocated_errors.erase(ptrToInt(error));
    chassert(erased);
}

bool isKernelErrorAllocation(void * error)
{
    std::lock_guard guard(mutex);
    return allocated_errors.contains(ptrToInt(error));
}

}

ffi::EngineError * KernelUtils::allocateError(ffi::KernelError etype, ffi::KernelStringSlice message)
{
    auto * error = new KernelError;
    error->etype = etype;
    error->error_message = std::string(message.ptr, message.len);
    recordKernelErrorAllocation(error);

    LOG_TRACE(
        getLogger("KernelUtils"),
        "Allocated KernelError (Pointer: {}, EType: {}, Message: {})",
        ptrToInt(error), magic_enum::enum_name(etype), error->error_message);

    return error;
}

[[noreturn]] void KernelUtils::throwError(ffi::EngineError * error, const std::string & from)
{
    if (isKernelErrorAllocation(error))
    {
        LOG_TRACE(
            getLogger("KernelUtils"),
            "Deallocating KernelError (Pointer: {})",
            ptrToInt(error));

        KernelError * kernel_error = static_cast<KernelError *>(error);
        auto error_message_copy = kernel_error->error_message;
        auto etype_copy = kernel_error->etype;
        delete kernel_error;
        forgetKernelErrorAllocation(error);

        throw DB::Exception(
            DB::ErrorCodes::DELTA_KERNEL_ERROR,
            "Received DeltaLake kernel error {}: {} (in {})",
            etype_copy, error_message_copy, from);
    }
    else
    {
        ffi::KernelError etype = error->etype;

        LOG_ERROR(
            getLogger("KernelUtils"),
            "Received unknown error from DeltaLake kernel. (Pointer: {}, EType: {}) (in {})",
            ptrToInt(error), etype, from);

        delete error;

        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Received unknown error from DeltaLake kernel. (Pointer: {}, EType: {}) (in {})",
            ptrToInt(error), etype, from);
    }
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
