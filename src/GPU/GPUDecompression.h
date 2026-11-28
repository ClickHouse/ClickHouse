#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUFuture.h>
#include <GPU/GPUResources.cuh>

#include <memory>

namespace DB::GPU
{

class CompressedHostColumn;

/// Expands compressed columns on the device, on one stream: `add` sends the blocks of each column there, and `expand`
/// expands all of them at once, with a call of nvcomp per codec.
class DecompressionBatch
{
public:
    explicit DecompressionBatch(cuda::stream_ref stream);
    ~DecompressionBatch();

    /// Sends the blocks of `column` to the device, to expand to `destination`: `column.expandedBytes()` bytes of device
    /// memory of the stream. `column` is read until `expand` returns.
    void add(const CompressedHostColumn & column, char * destination);

    /// Expands what was added, and waits for the device to check that every block expanded as its header says. The
    /// future is of the expanded bytes in their destinations.
    GPUFuture<void> expand();

private:
    struct State;
    std::unique_ptr<State> state;
};

}

#endif
