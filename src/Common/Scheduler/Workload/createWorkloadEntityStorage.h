#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>

namespace DB
{

std::unique_ptr<IWorkloadEntityStorage> createWorkloadEntityStorage(const ContextMutablePtr & global_context);

}
