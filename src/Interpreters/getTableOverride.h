#pragma once
#include <Parsers/IAST_fwd.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>

namespace DB
{
ASTPtr tryGetTableOverride(const String & mapped_database, const String & table, ContextPtr context);
}
