#pragma once
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Enable all experimental settings that can be used in CREATE query.
void enableAllExperimentalSettings(ContextMutablePtr context);

}
