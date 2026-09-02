#include <GPU/GPUDevice.h>
#include <GPU/GPUKernels.h>

namespace DB::GPU
{

namespace
{

Status fromC(int status)
{
    switch (status)
    {
        case CH_GPU_OK: return Status::Ok;
        case CH_GPU_NO_DEVICE: return Status::NoDevice;
        case CH_GPU_ALLOC_FAILED: return Status::AllocFailed;
        case CH_GPU_COPY_FAILED: return Status::CopyFailed;
        case CH_GPU_LAUNCH_FAILED: return Status::LaunchFailed;
        default: return Status::InvalidArgument;
    }
}

std::string_view name(Status status)
{
    switch (status)
    {
        case Status::Ok: return "ok";
        case Status::NoDevice: return "no CUDA device";
        case Status::AllocFailed: return "device allocation failed";
        case Status::CopyFailed: return "host/device copy failed";
        case Status::LaunchFailed: return "kernel launch failed";
        case Status::InvalidArgument: return "invalid argument";
    }
    return "unknown";
}

}

std::string describe(Status status)
{
    std::string result(name(status));

    /// The kernels keep the driver's own message in a thread-local buffer; it is the
    /// only part of a failure that says anything specific.
    if (const char * detail = ch_gpu_last_error(); detail != nullptr && detail[0] != '\0')
        result += " (" + std::string(detail) + ")";

    return result;
}

int deviceCount()
{
    return ch_gpu_device_count();
}

bool isAvailable()
{
    return ch_gpu_device_count() > 0;
}

std::string deviceName(int device)
{
    char buffer[256];
    if (ch_gpu_device_name(device, buffer, sizeof(buffer)) != CH_GPU_OK)
        return {};
    return buffer;
}

Status addInt64(std::span<const int64_t> a, std::span<const int64_t> b, std::span<int64_t> out)
{
    if (a.size() != b.size() || a.size() != out.size())
        return Status::InvalidArgument;

    return fromC(ch_gpu_add_int64(a.data(), b.data(), out.data(), a.size()));
}

Status sumInt64(std::span<const int64_t> data, int64_t & out)
{
    return fromC(ch_gpu_sum_int64(data.data(), data.size(), &out));
}

}
