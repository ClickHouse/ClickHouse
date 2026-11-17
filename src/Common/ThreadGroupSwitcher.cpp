#include <Common/ThreadGroupSwitcher.h>

namespace DB
{

ThreadGroupPtr getCurrentThreadGroup()
{
    if (!current_thread)
        return nullptr;
    return current_thread->getThreadGroup();
}

}
