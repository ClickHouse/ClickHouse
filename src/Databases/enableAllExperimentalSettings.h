#pragma once
#include <Interpreters/Context_fwd.h>

#include <string>
#include <vector>

namespace DB
{

/// Settings that allow experimental, deprecated or unsafe features in a CREATE query.
/// The list covers every gate that recovery needs: recoverLostReplica() enables all of them.
/// --dump-schema emits only those that the CREATE statements it dumps can reach.
const std::vector<std::string> & allExperimentalSettingNames();

/*
 * Enables all of the above on the given context.
 */
void enableAllExperimentalSettings(ContextMutablePtr context);

}
