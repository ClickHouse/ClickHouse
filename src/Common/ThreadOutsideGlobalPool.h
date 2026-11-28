#pragma once

#include <Common/ThreadPool.h>


/// An OS thread for a `ThreadPoolImpl` that does not take its threads from `GlobalThreadPool`.
/// Being a type of its own, unlike `GlobalThreadType`, makes such a pool report the local thread pool metrics.
class ThreadOutsideGlobalPool : public GlobalThreadType
{
public:
    using GlobalThreadType::GlobalThreadType;
};
