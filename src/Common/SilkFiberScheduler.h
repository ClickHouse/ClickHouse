#pragma once

#include "config.h"

#if USE_SILK

#include <cstdint>
#include <functional>
#include <string_view>
#include <utility>
#include <vector>

namespace silk
{
class Fiber;
class FiberFuture;
}

namespace Silk
{

/// We need larger stacks than the Silk library uses because
/// OpenSSL handshakes run on fiber stacks and need more room than the silk default.
inline constexpr uint32_t DEFAULT_FIBER_STACK_SIZE = 320 * 1024;

void initializeFiberScheduler(uint32_t fiber_stack_size);
void destroyFiberScheduler();

using FiberSwitchHook = void (*)(silk::Fiber *) noexcept;

/// The process supports a single `silk::FiberScheduler` instance, but fibers of more than
/// one kind run on it: fibers spawned through `spawn`/`runBlocking` above, and the reader
/// executor's fetch fibers (see `IO/SilkFiberJob.h`), which follow a different parameters
/// convention and need their own suspend/resume bookkeeping. The scheduler-wide switch hooks
/// installed by `initializeFiberScheduler` dispatch on the fiber category: they handle
/// `spawn`-created fibers themselves and forward fibers of the category registered here to
/// these hooks. Register before the first fiber of that category is spawned; the category
/// must be distinct from the one used by `spawn`.
void setFiberHooksForCategory(uint8_t category, FiberSwitchHook on_suspend, FiberSwitchHook on_resume);

bool isFiberSchedulerInitialized();

bool isInsideFiber();

using RuntimeCounters = std::vector<std::pair<std::string_view, uint64_t>>;

RuntimeCounters getRuntimeCounters();

[[nodiscard]] int spawn(std::function<int()> task, silk::FiberFuture & future);

/// Runs the task to completion: blocks a plain thread, suspends cooperatively when called from a fiber.
[[nodiscard]] int runBlocking(std::function<int()> task);

}

#else

namespace Silk
{

inline bool isInsideFiber()
{
    return false;
}

}

#endif
