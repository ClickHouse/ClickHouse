#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{

/** Build AST filter node for index analysis from WHERE and PREWHERE sections of select query and additional filters.
  * If select query does not have WHERE and PREWHERE and additional filters are empty null is returned.
  */
ASTPtr buildFilterNode(const ASTPtr & select_query, ASTs additional_filters = {});

}
