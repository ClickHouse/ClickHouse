#pragma once

#include <mutex>

namespace DB
{

using OnceFlag = std::once_flag;

template <typename Callable, typename ...Args>
void callOnce(OnceFlag & flag, Callable && func, Args&&... args)
{
    std::call_once(flag, std::forward<Callable>(func), std::forward<Args>(args)...);
}

}
