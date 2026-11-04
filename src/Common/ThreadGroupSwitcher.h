#pragma once

/// Convenience header that provides ThreadGroupSwitcher and getCurrentThreadGroup
/// without pulling in the full CurrentThread.h (which includes ThreadStatus.h).

#include <Common/ThreadStatus.h>

namespace DB
{

/// Returns the thread group of the current thread, or nullptr if not attached.
/// This is equivalent to CurrentThread::getGroup() but avoids including CurrentThread.h.
ThreadGroupPtr getCurrentThreadGroup();

}
