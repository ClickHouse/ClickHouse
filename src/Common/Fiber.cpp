#include <Common/Fiber.h>

Fiber::FiberPtr & Fiber::getCurrentFiber()
{
    thread_local static FiberPtr current_fiber;
    return current_fiber;
}
