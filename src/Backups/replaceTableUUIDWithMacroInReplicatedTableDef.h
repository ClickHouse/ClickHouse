#pragma once

#include <Core/UUID.h>

namespace DB
{

class ASTCreateQuery;

/// While making a replicated table it replaces "{uuid}" in zookeeper path with the real table UUID.
/// This function reverts this replacement.d
void replaceTableUUIDWithMacroInReplicatedTableDef(ASTCreateQuery & create_query, const UUID & table_uuid);

}
