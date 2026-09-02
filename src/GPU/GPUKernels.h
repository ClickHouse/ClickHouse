#pragma once

/// The ABI boundary between ClickHouse and the CUDA kernels.
///
/// ClickHouse is compiled by clang against the libc++ in contrib; the kernels are
/// compiled by nvcc, whose host pass runs gcc against the system libstdc++. The two
/// object files agree on the C ABI and on nothing else. So everything declared here
/// stays `extern "C"` and takes only POD arguments: no std:: types, no ClickHouse
/// types, no exceptions crossing over, no virtual dispatch, no ownership transfer.
/// Widening this interface to a C++ type would compile cleanly and then misbehave at
/// run time, which is the worst failure mode available.
///
/// This header is included from both sides, so it must stay valid C as well as C++.

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Kept as plain `int` at the boundary: an enum's underlying type is a choice the two
/// compilers make independently.
enum CHGpuStatus
{
    CH_GPU_OK = 0,
    CH_GPU_NO_DEVICE = 1,
    CH_GPU_ALLOC_FAILED = 2,
    CH_GPU_COPY_FAILED = 3,
    CH_GPU_LAUNCH_FAILED = 4,
    CH_GPU_INVALID_ARGUMENT = 5,
};

/// Number of usable CUDA devices, or 0 when the driver or the hardware is missing.
/// Never fails: a host without a GPU is a supported configuration.
int ch_gpu_device_count(void);

/// Name of `device`, written NUL-terminated into `out` and truncated to `out_size`.
/// Returns a CHGpuStatus.
int ch_gpu_device_name(int device, char * out, size_t out_size);

/// out[i] = a[i] + b[i] for i in [0, n), computed on the device.
/// The three host pointers must be distinct and hold at least `n` elements.
int ch_gpu_add_int64(const int64_t * a, const int64_t * b, int64_t * out, size_t n);

/// *out = sum(data[0..n)), computed on the device as a two-stage reduction.
/// An empty range yields 0. Wraps on overflow, matching ClickHouse's Int64 arithmetic.
int ch_gpu_sum_int64(const int64_t * data, size_t n, int64_t * out);

/// Detail of the most recent failure on the calling thread, for logging. Never null;
/// returns an empty string when nothing has failed yet. The buffer is thread-local and
/// is overwritten by the next failing call, so copy it if you need to keep it.
const char * ch_gpu_last_error(void);

#ifdef __cplusplus
}
#endif
