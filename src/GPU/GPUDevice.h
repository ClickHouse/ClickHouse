#pragma once

/// The clang side of the GPU engine: a thin, exception-free wrapper over the C entry
/// points in GPUKernels.h. This is where ClickHouse-facing code belongs -- column
/// marshalling, DB::Exception translation, device selection -- and none of it may leak
/// into the .cu files. See GPUKernels.h for why that boundary exists.
///
/// Deliberately free of ClickHouse dependencies for now, so the GPU targets build and
/// can be smoke-tested without the rest of the tree. When this starts touching
/// IColumn/Block, link clickhouse_common_io here and translate the status codes below
/// into DB::Exception at that point -- not inside the kernels.

#include <cstdint>
#include <span>
#include <string>

namespace DB::GPU
{

/// Mirrors CHGpuStatus without exposing the C enum to callers.
enum class Status
{
    Ok,
    NoDevice,
    AllocFailed,
    CopyFailed,
    LaunchFailed,
    InvalidArgument,
};

/// Text for `status`, plus the CUDA driver's own detail when the last call left any.
std::string describe(Status status);

/// Number of usable CUDA devices. 0 means the GPU path is unavailable, which is a
/// normal outcome rather than an error -- callers fall back to the CPU implementation.
int deviceCount();

/// True when at least one device is usable.
bool isAvailable();

/// Name of `device`, or an empty string if it cannot be queried.
std::string deviceName(int device = 0);

/// out[i] = a[i] + b[i]. Sizes must match; returns InvalidArgument otherwise.
Status addInt64(std::span<const int64_t> a, std::span<const int64_t> b, std::span<int64_t> out);

/// Sum of `data`, computed on the device. Empty input yields 0.
Status sumInt64(std::span<const int64_t> data, int64_t & out);

}
