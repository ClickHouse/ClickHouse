#pragma once

#include <Core/Names.h>
#include <Parsers/IAST_fwd.h>
#include <base/types.h>

namespace DB
{

/// Replace missed Object(Nullable('json')) subcolumns to NULL in query.
void replaceMissedSubcolumnsInQuery(ASTPtr & ast, const String & column_name);

/// Return true if the ASTFunction has missed object subcolumns.
/// Resolving ASTFunction independently is because we may lose the column name of missed object subcolumns.
/// For example, if `b.d` is a missed object subcolumn, the column name of `b.d * 2 + 3` will be `plus(multiply(NULL, 2), 3)`,
/// while we want to keep it as `plus(multiply(b.d, 2), 3)`.
bool replaceMissedSubcolumnsInFunction(ASTPtr & ast, const String & column_name);

}

