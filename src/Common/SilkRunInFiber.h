#pragma once

#if defined(OS_LINUX)

#include <functional>

namespace silk
{
class FiberFuture;
}

namespace Silk
{

void initializeFiberScheduler();
void destroyFiberScheduler();

[[nodiscard]] int RunInFiber(std::function<int()> task, silk::FiberFuture & future);
[[nodiscard]] int RunInFiber(std::function<int()> task);

}

#endif
