#pragma once

#include "config.h"

#if USE_SILK

#include <silk/fibers/fiber.h>

namespace DB
{

/// Options for the server-wide scheduler: enlarged fiber stacks (OpenSSL/AWS
/// SDK depth) and the ThreadGroup fiber-switch hooks. Shared by the server
/// startup path (SilkScheduler.cpp) and the unit-test environment so tests
/// exercise the same hooks. Kept out of SilkScheduler.h because the returned
/// type is Silk's: every includer of this header must be compiled in a target
/// that links ch_contrib::silk_fibers.
silk::FiberScheduler::Options makeServerSilkSchedulerOptions();

}

#endif
