#pragma once
#include <Interpreters/Context_fwd.h>

#include <string>
#include <vector>

namespace DB
{

/*
 * Settings that allow the use of experimental, deprecated, or potentially unsafe features in a
 * CREATE query. Anything that has to replay a stored CREATE without the settings it was created
 * under needs all of them: DatabaseReplicated::recoverLostReplica() sets them on a context, and
 * --dump-schema emits them as a prelude. New create-time gates belong here, so both stay complete.
 */
const std::vector<std::string> & allExperimentalSettingNames();

/*
 * Enables all of the above on the given context.
 */
void enableAllExperimentalSettings(ContextMutablePtr context);

}
