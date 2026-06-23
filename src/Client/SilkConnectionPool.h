#pragma once

#if defined(OS_LINUX)

#include <Client/ConnectionPool.h>

#include <silk/fibers/condvar.h>
#include <silk/fibers/mutex.h>

namespace Silk
{

using ConnectionPool = ::DB::ConnectionPool<silk::FiberMutex, silk::FiberCondVar>;

}

#endif
