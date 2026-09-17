#include <Common/StackfulCoroutine.h>

StackfulCoroutine::CoroutinePtr & StackfulCoroutine::getCurrentCoroutine()
{
    thread_local static CoroutinePtr current_coroutine;
    return current_coroutine;
}
