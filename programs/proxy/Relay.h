#pragma once

#include "config.h"

#if USE_SILK

#include "Backend.h"
#include "SocketIO.h"

namespace DB::Proxy
{

/// Forward @p initial_to_backend to the backend, then splice the two connections until either side
/// closes. Runs the two directions on separate fibers and updates the backend byte counters.
/// Both sockets are left closed on return.
void runRelay(
    FiberSocket & client,
    FiberSocket & backend_socket,
    Backend * backend,
    const String & initial_to_backend,
    size_t buffer_size);

}

#endif
