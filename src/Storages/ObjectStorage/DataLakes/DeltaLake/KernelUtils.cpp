#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>

#if USE_DELTA_KERNEL_RS
#include "delta_kernel_ffi.hpp"

#include <base/defines.h>
#include <base/EnumReflection.h>

#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Core/Field.h>

#include <Poco/String.h>
#include <fmt/ranges.h>
#include <filesystem>

namespace DB::ErrorCodes
{
    extern const int DELTA_KERNEL_ERROR;
    extern const int LOGICAL_ERROR;
}

namespace DeltaLake
{

std::string generateWritePath(const std::string & prefix, const std::string & format_str)
{
    return std::filesystem::path(prefix) / (DB::toString(DB::UUIDHelpers::generateV4()) + "." + Poco::toLower(format_str));
}

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

std::mutex mutex;
std::unordered_map<uintptr_t, std::string> allocated_errors;

uintptr_t ptrToInt(void * error)
{
    return reinterpret_cast<uintptr_t>(error);
}

void recordKernelErrorAllocation(uintptr_t error, std::string error_message)
{
    std::lock_guard guard(mutex);

    const auto [it, inserted] = allocated_errors.emplace(error, error_message);

    if (!inserted)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Trying to record already allocated error. (Current Pointer: {}, Current Message: {}, Previous Message: {})",
            error, error_message, it->second);
    }
}

std::string forgetKernelErrorAllocation(uintptr_t error)
{
    std::lock_guard guard(mutex);

    const auto it = allocated_errors.find(error);
    if (it == allocated_errors.end())
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Trying to deallocate unregistered error. (Pointer: {})", error);

    std::string recorded_message = std::move(it->second);
    allocated_errors.erase(it);

    return recorded_message;
}

bool isKernelErrorAllocation(uintptr_t error)
{
    std::lock_guard guard(mutex);
    return allocated_errors.contains(error);
}

}

ffi::EngineError * KernelUtils::allocateError(ffi::KernelError etype, ffi::KernelStringSlice message)
{
    auto * error = new ffi::EngineError;
    error->etype = etype;

    std::string error_message(message.ptr, message.len);
    recordKernelErrorAllocation(ptrToInt(error), error_message);

    LOG_TRACE(
        getLogger("KernelUtils"),
        "Allocated KernelError (Pointer: {}, EType: {}, Message: {})",
        ptrToInt(error), etype, error_message);

    return error;
}

[[noreturn]] void KernelUtils::throwError(ffi::EngineError * error, const std::string & from)
{
    if (error == nullptr)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unpacking nullptr DeltaLake kernel error. (in: {})", from);

    uintptr_t error_ptr = ptrToInt(error);

    if (isKernelErrorAllocation(error_ptr))
    {
        LOG_TRACE(
            getLogger("KernelUtils"),
            "Deallocating KernelError (Pointer: {}) (in {})",
            error_ptr, from);

        ffi::KernelError etype = error->etype;
        delete error;

        std::string error_message = forgetKernelErrorAllocation(error_ptr);

        throw DB::Exception(
            DB::ErrorCodes::DELTA_KERNEL_ERROR,
            "Received DeltaLake kernel error {}: {} (in {})",
            etype, error_message, from);
    }
    else
    {
        ffi::KernelError etype = error->etype;
        delete error;

        LOG_ERROR(
            getLogger("KernelUtils"),
            "Received unknown error from DeltaLake kernel. (Pointer: {}, EType: {}) (in {})",
            error_ptr, etype, from);

        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Received unknown error from DeltaLake kernel. (Pointer: {}, EType: {}) (in {})",
            error_ptr, etype, from);
    }
}

std::optional<std::string> tryGetPhysicalName(const std::string & name, const DB::NameToNameMap & physical_names_map)
{
    if (physical_names_map.empty())
        return name;

    auto it = physical_names_map.find(name);
    if (it == physical_names_map.end())
        return std::nullopt;
    return it->second;
}

std::string getPhysicalName(const std::string & name, const DB::NameToNameMap & physical_names_map)
{
    auto physical_name = tryGetPhysicalName(name, physical_names_map);
    if (!physical_name)
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
    return *physical_name;
}

}

#endif
