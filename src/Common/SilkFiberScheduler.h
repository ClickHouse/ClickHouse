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
class FiberFuture;
}

namespace Silk
{

/// We need larger stacks than the Silk library uses because
/// OpenSSL handshakes run on fiber stacks and need more room than the silk default.
inline constexpr uint32_t DEFAULT_FIBER_STACK_SIZE = 320 * 1024;

void initializeFiberScheduler(uint32_t fiber_stack_size);
void destroyFiberScheduler();

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
