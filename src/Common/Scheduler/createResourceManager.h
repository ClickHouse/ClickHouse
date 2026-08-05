#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/Scheduler/IResourceManager.h>

namespace DB
{

ResourceManagerPtr createResourceManager(const ContextMutablePtr & global_context);

}
