#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUTypes.cuh>

#include <utility>

namespace DB::GPU
{

namespace detail
{

inline bool isReady(const EventPtr & ready)
{
    const cudaError_t status = cudaEventQuery(ready.get());
    if (status == cudaErrorNotReady)
        return false;
    checkCuda(status, "Cannot ask the device whether a value is ready");
    return true;
}

inline void waitOn(const EventPtr & ready, cuda::stream_ref stream)
{
    checkCuda(cudaStreamWaitEvent(stream.get(), ready.get(), 0), "Cannot make a stream wait for a value of the device");
}

inline void wait(const EventPtr & ready)
{
    checkCuda(cudaEventSynchronize(ready.get()), "Cannot wait for a value of the device");
}

inline EventPtr recordOn(cuda::stream_ref stream)
{
    EventPtr ready = createEvent();
    checkCuda(cudaEventRecord(ready.get(), stream.get()), "Cannot mark a point in a stream of the device");
    return ready;
}

}

/// A value that work queued on a stream of the device makes ready: it is there once the device has done what was queued
/// before `ready` was recorded. Its owner waits for it on the device, by `waitOn`, or on the host, by `get`.
template <typename T>
class GPUFuture
{
public:
    GPUFuture(T value_, EventPtr ready_)
        : value(std::move(value_))
        , ready(std::move(ready_))
    {
    }

    /// Whether the device has made it ready, without waiting.
    bool isReady() const { return detail::isReady(ready); }

    /// The value, for the work queued on `stream` from now on, which waits for it on the device; the host does not wait.
    T waitOn(cuda::stream_ref stream) &&
    {
        detail::waitOn(ready, stream);
        return std::move(value);
    }

    /// The value once it is ready, waiting for it on the host.
    T get() &&
    {
        detail::wait(ready);
        return std::move(value);
    }

private:
    T value;
    EventPtr ready;
};

/// The producer side of `GPUFuture`: a value that work queued on `stream` makes. The producer fills it while it queues
/// that work, and `fulfill` hands it over as a future, ready once the device has done the work queued until then.
template <typename T>
class GPUPromise
{
public:
    GPUPromise(T value_, cuda::stream_ref stream_)
        : promised(std::move(value_))
        , stream(stream_)
    {
    }

    T & value() { return promised; }

    GPUFuture<T> fulfill() && { return GPUFuture<T>(std::move(promised), detail::recordOn(stream)); }

private:
    T promised;
    const cuda::stream_ref stream;
};

/// That work queued on a stream of the device is done, of no value.
template <>
class GPUFuture<void>
{
public:
    explicit GPUFuture(EventPtr ready_)
        : ready(std::move(ready_))
    {
    }

    bool isReady() const { return detail::isReady(ready); }

    /// The work queued on `stream` from now on waits for it on the device.
    void waitOn(cuda::stream_ref stream) && { detail::waitOn(ready, stream); }

    /// Waits for it on the host.
    void get() && { detail::wait(ready); }

private:
    EventPtr ready;
};

/// The promise that the work queued on `stream` until `fulfill` is done.
template <>
class GPUPromise<void>
{
public:
    explicit GPUPromise(cuda::stream_ref stream_)
        : stream(stream_)
    {
    }

    cuda::stream_ref on() const { return stream; }

    GPUFuture<void> fulfill() && { return GPUFuture<void>(detail::recordOn(stream)); }

private:
    const cuda::stream_ref stream;
};

}

#endif
