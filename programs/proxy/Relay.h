#pragma once

#include "config.h"

#if USE_SILK

#include <Backend.h>
#include <SocketIO.h>

namespace DB::Proxy
{

/// Forward @p initial_to_backend to the backend, then relay the two connections until both directions
/// have ended. A clean end of stream in one direction is forwarded as a half-close (for plaintext
/// destinations), an error tears down both. Runs the two directions on separate fibers and updates the
/// backend byte counters. Throws if the second fiber cannot be allocated.
///
/// @p relay_timeout_ms is applied to both sockets: a frontend reads the handshake under the much
/// shorter handshake timeout, but the relayed session that follows is long-lived, and an idle gap
/// between commands must not tear it down.
void runRelay(
    FiberSocket & client,
    FiberSocket & backend_socket,
    Backend * backend,
    const String & initial_to_backend,
    size_t buffer_size,
    UInt64 relay_timeout_ms);

}

#endif
