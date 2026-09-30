#pragma once
#include <Interpreters/Context_fwd.h>

#include <string>
#include <vector>

namespace DB
{

/// Settings that allow experimental, deprecated or unsafe features in a CREATE query.
/// recoverLostReplica() enables all of them, and --dump-schema emits those its CREATEs can reach.
const std::vector<std::string> & allExperimentalSettingNames();

/*
 * Enables all of the above on the given context.
 */
void enableAllExperimentalSettings(ContextMutablePtr context);

}
