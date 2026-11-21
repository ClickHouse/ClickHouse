#pragma once

#include "config.h"

#if USE_GPU

#include <boost/noncopyable.hpp>

#include <cstdint>
#include <memory>

namespace DB::GPU
{

/// How busy the device is, from the performance monitors of the GPU read by CUPTI's PM sampling: the share of the SMs that
/// are active and of the cycles their tensor pipes are, both of the whole device, every `interval_ms`. Each tenth of a
/// second the active time is accounted to the queries that hold device memory then (see `GPUMemoryTracking.h`), in equal
/// shares if there are several, as the profile events `GPUSMActiveMicroseconds` and `GPUTensorActiveMicroseconds`.
///
/// CUPTI is loaded at run time, from the CUDA toolkit found by the dynamic loader, and the driver lets only a process with
/// `CAP_SYS_ADMIN` read the counters unless it is loaded with `NVreg_RestrictProfilingToAdminUsers=0`. Without either the
/// sampler does not start and says why in the log.
class GPUActivitySampler : private boost::noncopyable
{
public:
    /// Null if the activity cannot be sampled.
    static std::unique_ptr<GPUActivitySampler> tryStart(uint64_t interval_ms);
    ~GPUActivitySampler();

private:
    struct State;
    explicit GPUActivitySampler(std::unique_ptr<State> state_);
    std::unique_ptr<State> state;
};

}

#endif
