#pragma once
#include <Interpreters/Context_fwd.h>

namespace DB
{

/*
 * Enables all settings that allow the use of experimental, deprecated, or potentially unsafe features
 * in a CREATE query. This function is used in DatabaseReplicated::recoverLostReplica() to create tables
 * when the original settings used to create the table are not available.
 */

void enableAllExperimentalSettings(ContextMutablePtr context);

}
